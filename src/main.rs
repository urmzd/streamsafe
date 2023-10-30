use gst::{
    glib::{self, RustClosure, Value},
    prelude::*,
    MessageView, State,
};
use gstreamer as gst;
use std::{io, sync::{atomic::AtomicU32, Arc, Mutex}, thread, time::Duration};
use termion::{input::TermRead, raw::IntoRawMode};
use tracing::{error, info};

// Algorithm:
//  Global:
//      - frame_buffer
//  Thread 1:
//      - produce png frames into frame_buffer
//      - on 300 frames, send_frames_to_collector(frames[frame_start..=frame_end])
//  Thread 2:
//  the collector should do the following:
//      - create mp4 from png files
//      - in prallel:
//          - hash frames
//      on hash_complete:
//          - create video_segment_metadata
//      on_mp4_complete:
//          - upload video_segment to storage
//          - upload video_segment_metadata to storage
//
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


    // Elements
    let src = gst::ElementFactory::make("rtspsrc").build()?;
    let depay = gst::ElementFactory::make("rtph265depay").build()?;
    // Ensures that we're reading from the default stream.
    src.set_property_from_str("location", &rtsp_uri);
    src.set_property_from_str("protocols", "tcp");

    let parse = gst::ElementFactory::make("h265parse").build()?;
    //let decode = gst::ElementFactory::make("avdec_h265").build()?;
    //let convert = gst::ElementFactory::make("videoconvert").build()?;
    //let encoder = gst::ElementFactory::make("x264enc").build()?;
    let sink = gst::ElementFactory::make("splitmuxsink").build()?;

    let max_size_time = Duration::from_secs(1);

    sink.set_property_from_str("max-size-time", &max_size_time.as_nanos().to_string());
    sink.set_property_from_str("location", "frames/output%05d.mp4");

    let pipeline = gst::Pipeline::with_name("test-pipeline");
    let links = [
        &src, &depay, &parse, //&decode,
        //&convert,
        //&encoder,
        &sink,
    ];
    pipeline.add_many(&links)?;
    gst::Element::link_many(&links[1..])?;

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

    // we add video names to this buffer
    let video_buffer = Arc::new(Mutex::new(Vec::new()));
    let counter = AtomicU32::new(0);

    let video_buffer_clone = Arc::clone(&video_buffer);

    // spawn a thread that consumes frames from the video_buffer_clone

    sink.connect_closure(
        "format-location-full",
        false,
        RustClosure::new(move |value| {
            let internal_sink = value.get(0).unwrap().get::<gst::Element>();
            let fragment_id = value.get(1).unwrap().get::<u32>().unwrap();
            let sample = value.get(2).unwrap().get::<gst::Sample>().unwrap();

            let new_count = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            let new_location = format!("frames/frame{}.mp4", new_count);
            let mut buffer = video_buffer_clone.lock().unwrap();
            buffer.push(new_location.clone());

            let new_location_value: Value = new_location.into();
            let ret_value = Some(new_location_value);

            error!("fragment_id: {}", fragment_id);
            error!("sample: {:?}", sample);

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
                error!("Made Element {:?}", element);
            }
            _ => {}
        }

        glib::ControlFlow::Continue
    });

    main_loop.run();

    pipeline.set_state(State::Null)?;

    Ok(())
}
