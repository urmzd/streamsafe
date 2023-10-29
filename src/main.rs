use gst::{glib::RustClosure, prelude::*, FlowReturn};
use gstreamer as gst;
use tracing::info;

pub const DEFAULT_RTSP_URI: &str = "rtsp://localhost:8554/vsa";
pub const FRAMES: u32 = 300;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    tracing_gstreamer::integrate_events();

    gstreamer::debug_set_default_threshold(gstreamer::DebugLevel::Memdump);
    gst::debug_remove_default_log_function();

    gst::init()?;

    // Elements
    let src = gst::ElementFactory::make("rtspsrc").build()?;
    // Ensures that we're reading from the default stream.
    src.set_property_from_str("location", DEFAULT_RTSP_URI);
    src.set_property_from_str("protocols", "tcp");

    let depay = gst::ElementFactory::make("rtph264depay").build()?;
    let parse = gst::ElementFactory::make("h264parse").build()?;
    let decode = gst::ElementFactory::make("avdec_h264").build()?;
    let convert = gst::ElementFactory::make("videoconvert").build()?;
    // lossless compression
    let encoder = gst::ElementFactory::make("pngenc").build()?;
    //let sink = gst::ElementFactory::make("multifilesink").build()?;
    //sink.set_property_from_str("location", "img%d.jpeg");
    let sink = gst::ElementFactory::make("appsink").build()?;
    // Allows us to use the 'appsink' element to get frames and metadata
    sink.set_property("emit-signals", &true);
    sink.set_property("max-buffers", &FRAMES);

    let pipeline = gst::Pipeline::with_name("test-pipeline");
    let links = [&src, &depay, &parse, &decode, &convert, &encoder, &sink];
    pipeline.add_many(&links)?;
    gst::Element::link_many(&links[1..])?;

    sink.connect_closure(
        "new-sample",
        false,
        RustClosure::new(|value| {
            // cast value[0] to GstElement
            //let internal_app_sink = value.get(0).downcast_ref::<gst::Element>();
            let internal_app_sink = value.get(0).unwrap().get::<gst::Element>().unwrap();
            let sample = internal_app_sink.emit_by_name::<gst::Sample>("pull-sample", &[]);

            info!("Received sample: {:?}", sample);

            // get the sample
            Some(FlowReturn::Ok.into())
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
