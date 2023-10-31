use chrono::{self, Utc};
use gst::{
    glib::{self, RustClosure, Value},
    prelude::*,
    MessageView, PadProbeType, State,
};
use gstreamer as gst;
use std::{
    io,
    sync::atomic::{AtomicU32, Ordering},
    thread,
    time::Duration,
};
use termion::{input::TermRead, raw::IntoRawMode};
use tracing::{error, info};

pub const DEFAULT_RTSP_URI: &str = "localhost:8554/vsa";
pub const FRAMES: u32 = 300;

#[derive(PartialEq, Eq, Copy, Clone)]
enum Command {
    Quit,
}

fn handle_keyboard(ready_tx: glib::Sender<Command>) {
    let _stdout = io::stdout().into_raw_mode().unwrap();
    let mut stdin = termion::async_stdin().keys();

    loop {
        if let Some(Ok(input)) = stdin.next() {
            let command = match input {
                _ => Command::Quit,
            };

            ready_tx
                .send(command)
                .expect("failed to send command through channel");

            if command == Command::Quit {
                break;
            }
        }

        thread::sleep(Duration::from_millis(50));
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    tracing_gstreamer::integrate_events();

    gstreamer::debug_set_default_threshold(gstreamer::DebugLevel::Memdump);
    gst::debug_remove_default_log_function();

    gst::init()?;

    let base_rtsp_uri = std::env::var("RTSP_URI").unwrap_or(DEFAULT_RTSP_URI.to_owned());
    let rtsp_uri = format!("rtsp://{base_rtsp_uri}", base_rtsp_uri = base_rtsp_uri);

    info!("RTSP URI: {}", rtsp_uri);

    let video_format = "h264";

    // Elements
    let src = gst::ElementFactory::make("rtspsrc").build()?;
    //let buffer = gst::ElementFactory::make("rtpjitterbuffer").build()?;
    let depay = gst::ElementFactory::make(&format!("rtp{}depay", &video_format)).build()?;
    let parse = gst::ElementFactory::make(&format!("{}parse", &video_format)).build()?;
    let sink = gst::ElementFactory::make("splitmuxsink").build()?;
    //let decode = gst::ElementFactory::make(&format("avdec_{}", &video_format)).build()?;
    //let convert = gst::ElementFactory::make("videoconvert").build()?;
    //let encoder = gst::ElementFactory::make("x264enc").build()?;

    // disable automatic splits
    // Ensures that we're reading from the default stream.
    src.set_property_from_str("location", &rtsp_uri);
    src.set_property_from_str("protocols", "tcp");
    sink.set_property_from_str("max-size-time", "0");
    sink.set_property_from_str("max-size-bytes", "0");
    //sink.set_property_from_str("location", "segments/segment-%05d.mp4");

    let pipeline = gst::Pipeline::with_name("test-pipeline");
    let links = [
        &src,
        //&buffer,
        &depay,
        &parse,
        //&decode,
        //&convert,
        //&encoder,
        &sink,
    ];
    pipeline.add_many(&links)?;
    gst::Element::link_many(&links[1..])?;

    let frame_count = AtomicU32::new(0);
    let sink_clone = sink.clone();

    let parse_src_pad = parse
        .static_pad("sink")
        .expect("sink pad to exist on parse");

    parse_src_pad
        .add_probe(
            PadProbeType::BUFFER | PadProbeType::PUSH,
            move |_pad, _pad_probe| {
                let current_frame_idx = frame_count.load(Ordering::Relaxed);

                info!("we are on frame {}", current_frame_idx);

                if current_frame_idx == FRAMES {
                    sink_clone.emit_by_name::<()>("split-after", &[]);
                    info!("we have requested to split");
                    frame_count.store(0, Ordering::Relaxed);
                } else {
                    frame_count.fetch_add(1, Ordering::Relaxed);
                };

                gst::PadProbeReturn::Ok
            },
        )
        .expect("probe callback to have been added");

    src.connect_pad_added(move |src, src_pad| {
        src.downcast_ref::<gst::Bin>()
            .unwrap()
            .debug_to_dot_file_with_ts(gst::DebugGraphDetails::all(), "pad-added");

        let sink_pad = depay.static_pad("sink").unwrap();

        if src_pad.is_linked() {
            info!("We are already linked. Ignoring.");
            return;
        }

        let new_pad_caps = src_pad.current_caps().unwrap();
        let new_pad_struct = new_pad_caps.structure(0).unwrap();
        let new_pad_type = new_pad_struct.name();

        let res = src_pad.link(&sink_pad);
        if res.is_err() {
            info!("Link failed (type: {:?}).", new_pad_type);
        } else {
            info!("Link succeeded (type: {:?}).", new_pad_type);
        }
    });

    let counter = AtomicU32::new(0);

    sink.connect_closure(
        "format-location-full",
        false,
        RustClosure::new(move |_value| {
            //let fragment_id = value.get(1).unwrap().get::<u32>().unwrap();
            //let sample = value.get(2).unwrap().get::<gst::Sample>().unwrap();

            let new_count = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            let new_location = format!("segments/segment-{}.mp4", new_count);

            let new_location_value: Value = new_location.into();
            let ret_value = Some(new_location_value);

            // log current time utc
            error!("{}", Utc::now());
            //error!("fragment_id: {}", fragment_id);
            //error!("sample: {:?}", sample);

            ret_value
        }),
    );

    pipeline.set_state(gst::State::Playing)?;

    let main_context = glib::MainContext::default();

    let _guard = main_context.acquire().unwrap();

    let (g_tx, g_rx) = glib::MainContext::channel(glib::Priority::DEFAULT);

    thread::spawn(move || handle_keyboard(g_tx));

    let main_loop = glib::MainLoop::new(None, false);
    let main_loop_clone = main_loop.clone();
    let context_weak_pipeline = pipeline.downgrade();
    //let bus_weak_pipeline = pipeline.downgrade();

    g_rx.attach(Some(&main_context), move |cmd: Command| {
        let _pipeline = match context_weak_pipeline.upgrade() {
            Some(pipeline) => pipeline,
            None => return glib::ControlFlow::Continue,
        };

        match cmd {
            Command::Quit => {
                main_loop_clone.quit();
            }
        }

        glib::ControlFlow::Continue
    });

    let bus = pipeline.bus().expect("pipeline received");

    let _bus_watch = bus.add_watch(move |_, msg| {
        match msg.view() {
            MessageView::Element(element) => {
                info!("received element {:?}", element);
            }
            _ => {}
        }

        glib::ControlFlow::Continue
    });

    main_loop.run();

    pipeline.set_state(State::Null)?;

    Ok(())
}

//struct Publisher {}
//impl Publisher {
//// args: video_path
//fn publish() {}
//}
//struct FrameBuffer {}
//impl FrameBuffer {
//// args: frame
//fn add_frame() {}
//}
//// video buffer contains a list of videos that get translated into frames
//struct VideoBuffer {}
