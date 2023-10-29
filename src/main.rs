use gst::{glib::RustClosure, prelude::*};
use gstreamer as gst;
use tracing::info;

const DEFAULT_RTSP_URI: &str = "rtsp://10.101.8.51:554/tcp/av0_1";
const FRAMES_TO_COLLECT: usize = 300; // Set X frames to collect

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    tracing_gstreamer::integrate_events();

    gstreamer::debug_set_default_threshold(gstreamer::DebugLevel::Memdump);
    gst::debug_remove_default_log_function();

    gst::init()?;

    let src = gst::ElementFactory::make("rtspsrc").build()?;
    let depay = gst::ElementFactory::make("rtph265depay").build()?;
    let parse = gst::ElementFactory::make("h265parse").build()?;
    let decode = gst::ElementFactory::make("avdec_h265").build()?;
    let convert = gst::ElementFactory::make("videoconvert").build()?;
    let enc = gst::ElementFactory::make("x264enc").build()?; // Replace 'pngenc' with 'x264enc'
    let mux = gst::ElementFactory::make("mp4mux").build()?; // Add an MP4 multiplexer element
    let sink = gst::ElementFactory::make("appsink").build()?;

    let pipeline = gst::Pipeline::with_name("test-pipeline");
    pipeline.add_many(&[&src, &depay, &parse, &decode, &convert, &enc, &mux, &sink])?;
    gst::Element::link_many(&[&depay, &parse, &decode, &convert, &enc, &mux, &sink])?;

    // Allows us to use the 'appsink' element to get frames and metadata
    sink.set_property("emit-signals", &true);

    // Ensures that we're reading from the default stream.
    src.set_property_from_str("location", DEFAULT_RTSP_URI);



    let sink_clone = sink.clone();
    sink_clone.connect_closure(
        "new-sample",
        false,
        RustClosure::new(|value| {
            // g_signal_emit_by_name 
            //gst::g_signal_emit_by_name()
            


            // let sample = sink_clone.emit_by_name::<gst::Sample>("pull-sample", &[]);

            // get the sample

            
            None
        }),
    );

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

    pipeline.set_state(gst::State::Playing)?;

    let bus = pipeline.bus().unwrap();

    for msg in bus.iter_timed(gst::ClockTime::NONE) {
        use gst::MessageView;

        match msg.view() {
            MessageView::Error(err) => {
                info!(
                    "Error received from element {:?}: {} ({:?})",
                    err.src().map(|s| s.path_string()),
                    err.error(),
                    err.debug()
                );
            }
            MessageView::Eos(_) => {
                info!("End of stream reached.");
                break;
            }
            _ => {}
        }
    }

    pipeline.set_state(gst::State::Null)?;

    Ok(())
}

struct Metadata {
    // Define the structure of metadata here
}
