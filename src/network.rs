use constants::BUFFER_SIZE;
use std::io::Read;
use std::io::Result;
use std::net::TcpStream;

pub trait NetworkRead {
    fn read_to_message_end(&mut self, buf: &mut Vec<u8>) -> Result<usize>;
}

impl NetworkRead for TcpStream {
    fn read_to_message_end(&mut self, buf: &mut Vec<u8>) -> Result<usize> {
        let mut total_size: usize = 0;
        loop {
            let mut fixed_buf = [0; BUFFER_SIZE];
            info!("before read_to_end");
            let bytes_read = try!(self.read(&mut fixed_buf));
            buf.extend(fixed_buf[..bytes_read].iter());
            info!("read {:?} bytes", bytes_read);
            total_size += bytes_read;
            if bytes_read != BUFFER_SIZE || bytes_read == 0 {
                break;
            }
        }
        Ok(total_size)
    }
}
