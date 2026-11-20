use std::{
    cmp::min,
    collections::VecDeque,
    io::{Error, Read},
};

extern "C" {
    fn open_connection(ptr_to_url: u64, length_url: u64, soft_revocation_mode: u8) -> i64;
    fn read_from_internet(identifier: i64, ptr_to_write: u64) -> i64; // will return -1 if fail
    fn close_connection(identifier: i64) -> u32;
    fn buffer_size() -> u64;
}

pub struct Internet {
    id: i64,
    buffer: VecDeque<u8>,
    size: usize,
}

impl Internet {
    pub fn new(url: &str, soft_revocation_mode: bool) -> Result<Self, Error> {
        let id = unsafe {
            open_connection(url.as_ptr() as u64,
                            url.len() as u64,
                            soft_revocation_mode as u8)
        };
        if id < 0 {
            return Err(Error::new(
                std::io::ErrorKind::Other,
                "Connection cannot be opened",
            ));
        }
        let raw_size = unsafe { buffer_size() } as usize;
        let size = raw_size.clamp(1, 1024 * 1024);
        Ok(Self {
            id,
            buffer: VecDeque::new(),
            size,
        })
    }

    unsafe fn load_buffer(&mut self) -> std::io::Result<()> {
        let mut ret = self.size as i64;
        let mut subarray = Vec::new();
        subarray.resize(self.size, 0);
        // Read chunks into buffer with strict negative and upper-bound checks
        while ret > 0 && self.buffer.len() < self.size {
            ret = read_from_internet(self.id, subarray.as_mut_ptr() as u64);
            if ret < 0 {
                return Err(Error::new(std::io::ErrorKind::Other, "File cannot be read"));
            }
            if ret == 0 {
                break;
            }
            let n = min(ret as usize, subarray.len());
            self.buffer
                .append(&mut VecDeque::from(subarray[..n].to_vec()));
        }
        Ok(())
    }
}

impl Read for Internet {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut i = 0;
        let mut len = buf.len();
        while i < buf.len() && len > 0 {
            unsafe {
                self.load_buffer()?;
            }
            len = min(self.buffer.len(), buf.len() - i);
            for x in 0..len {
                buf[i + x] = self.buffer.pop_front().unwrap();
            }
            i += len;
        }
        Ok(i)
    }
}

impl Drop for Internet {
    fn drop(&mut self) {
        unsafe {
            close_connection(self.id);
        }
    }
}
