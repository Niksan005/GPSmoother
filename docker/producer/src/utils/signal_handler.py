import logging
import signal
from typing import Callable

logger = logging.getLogger(__name__)

class SignalHandler:
    """Responsible for handling system signals for graceful shutdown."""
    
    def __init__(self, stop_callback: Callable[[], None]):
        """
        Initialize the signal handler.
        
        Args:
            stop_callback: Function to call when a shutdown signal is received
        """
        self.stop_callback = stop_callback
        self.running = True
    
    def setup(self) -> None:
        """Set up signal handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
        logger.info("Signal handlers set up for graceful shutdown")
    
    def _handle_signal(self, signum: int, frame) -> None:
        """
        Handle shutdown signals.
        
        Args:
            signum: Signal number
            frame: Current stack frame
        """
        logger.info(f"Received signal {signum}")
        self.running = False
        self.stop_callback()
    
    def is_running(self) -> bool:
        """
        Check if the application should continue running.
        
        Returns:
            True if the application should continue running, False otherwise
        """
        return self.running 