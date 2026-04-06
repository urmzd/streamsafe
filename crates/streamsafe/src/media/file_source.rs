use super::Frame;
use crate::error::Result;
use crate::source::Source;
use std::path::PathBuf;

/// Reads local video files and yields `Frame` variants.
///
/// **Not yet implemented.** Use [`RtspSource`](super::RtspSource) for now.
pub struct FileSource {
    _path: PathBuf,
}

impl FileSource {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            _path: path.into(),
        }
    }
}

impl Source for FileSource {
    type Output = Frame;

    async fn produce(&mut self) -> Result<Option<Frame>> {
        todo!("FileSource not yet implemented — use RtspSource or contribute!")
    }
}
