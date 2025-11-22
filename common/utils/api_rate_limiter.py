from common.utils.logger_config import logger
import time

class RateLimiter:
    def __init__(self, max_calls_per_minute):
        self.max_calls = max_calls_per_minute
        self.calls = []
    
    def wait_if_needed(self):
        now = time.time()
        self.calls = [call_time for call_time in self.calls if (now - call_time) < 60] # Remove calls over 1 minute
        if len(self.calls) >= self.max_calls:
            sleep_time = 60 - (now - self.calls[0]) # So that max calls in 60 seconds period = max_calls
            logger.info(f"Reach rate limited, watiting {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
            
        self.calls.append(now)