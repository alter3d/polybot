"""Console notification output for trading opportunities.

This module provides console-based notification output with formatted,
colored messages for easy visibility of detected trading opportunities.
"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

from src.market.opportunity_detector import Opportunity

logger = logging.getLogger(__name__)


# ANSI color codes for terminal output
class Colors:
    """ANSI escape codes for terminal colors."""

    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"

    # Foreground colors
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"

    # Background colors
    BG_GREEN = "\033[42m"
    BG_YELLOW = "\033[43m"
    BG_BLUE = "\033[44m"

    @classmethod
    def colorize(cls, text: str, *colors: str) -> str:
        """Apply color codes to text.

        Args:
            text: The text to colorize.
            *colors: Color codes to apply.

        Returns:
            Text wrapped with ANSI color codes.
        """
        color_codes = "".join(colors)
        return f"{color_codes}{text}{cls.RESET}"


class BaseNotifier(ABC):
    """Abstract base class for notification handlers.

    Provides a common interface for different notification backends
    (console, email, Discord, etc.) to implement.
    """

    @abstractmethod
    def notify(self, opportunity: Opportunity) -> bool:
        """Send a notification for a detected opportunity.

        Args:
            opportunity: The opportunity to notify about.

        Returns:
            True if notification was sent successfully, False otherwise.
        """
        pass

    @abstractmethod
    def notify_batch(self, opportunities: list[Opportunity]) -> int:
        """Send notifications for multiple opportunities.

        Args:
            opportunities: List of opportunities to notify about.

        Returns:
            Number of notifications sent successfully.
        """
        pass


class ConsoleNotifier(BaseNotifier):
    """Console-based notification output with colored formatting.

    Outputs formatted opportunity notifications to the console with
    ANSI color codes for improved visibility. Suitable for terminal
    monitoring and development.

    Attributes:
        use_colors: Whether to use ANSI color codes in output.
        prefix: Optional prefix string for all notifications.
    """

    def __init__(
        self,
        use_colors: bool = True,
        prefix: Optional[str] = None,
    ) -> None:
        """Initialize the console notifier.

        Args:
            use_colors: Enable colored output (default True).
            prefix: Optional prefix for notification lines.
        """
        self.use_colors = use_colors
        self.prefix = prefix or "🔔"

    def _format_timestamp(self, dt: datetime) -> str:
        """Format a datetime for display.

        Args:
            dt: The datetime to format.

        Returns:
            Formatted timestamp string (HH:MM:SS).
        """
        return dt.strftime("%H:%M:%S")

    def _format_price(self, price: float) -> str:
        """Format a price for display.

        Args:
            price: The price value to format.

        Returns:
            Formatted price string with dollar sign.
        """
        return f"${price:.2f}"

    def _get_side_display(self, side: str) -> str:
        """Get the display string for a trading side.

        Args:
            side: The trading side ("YES" or "NO").

        Returns:
            Colored or plain side indicator.
        """
        if not self.use_colors:
            return f"[{side}]"

        if side == "YES":
            return Colors.colorize(f"[{side}]", Colors.BOLD, Colors.GREEN)
        else:
            return Colors.colorize(f"[{side}]", Colors.BOLD, Colors.RED)

    def _get_source_display(self, source: str) -> str:
        """Get the display string for a price source.

        Args:
            source: The price source ("bid" or "last_trade").

        Returns:
            Formatted source indicator.
        """
        source_map = {
            "bid": "BID",
            "last_trade": "LAST TRADE",
        }
        display = source_map.get(source, source.upper())

        if not self.use_colors:
            return f"({display})"

        return Colors.colorize(f"({display})", Colors.DIM, Colors.CYAN)

    def _format_opportunity(self, opportunity: Opportunity) -> str:
        """Format an opportunity for console display.

        Creates a formatted string with all opportunity details including
        market ID, side, price, timestamp, and source type.

        Args:
            opportunity: The opportunity to format.

        Returns:
            Formatted notification string.

        Example output:
            🔔 OPPORTUNITY | [YES] $0.75 | Market: btc-15min | 14:32:15 (BID)
        """
        timestamp = self._format_timestamp(opportunity.detected_at)
        price = self._format_price(opportunity.price)
        side = self._get_side_display(opportunity.side)
        source = self._get_source_display(opportunity.source)

        # Format the header
        if self.use_colors:
            header = Colors.colorize("OPPORTUNITY", Colors.BOLD, Colors.YELLOW)
        else:
            header = "OPPORTUNITY"

        # Format market ID (truncate if too long)
        market_display = opportunity.market_id
        if len(market_display) > 40:
            market_display = market_display[:37] + "..."

        if self.use_colors:
            market_label = Colors.colorize("Market:", Colors.DIM)
            price_colored = Colors.colorize(price, Colors.BOLD, Colors.WHITE)
        else:
            market_label = "Market:"
            price_colored = price

        return (
            f"{self.prefix} {header} | "
            f"{side} {price_colored} | "
            f"{market_label} {market_display} | "
            f"{timestamp} {source}"
        )

    def notify(self, opportunity: Opportunity) -> bool:
        """Print an opportunity notification to the console.

        Formats and outputs the opportunity with colored formatting
        for easy visibility.

        Args:
            opportunity: The opportunity to notify about.

        Returns:
            True (console output always succeeds unless exception).

        Example:
            >>> from datetime import datetime
            >>> opp = Opportunity('btc-15min', 'YES', 0.75, datetime.now(), 'bid')
            >>> notifier = ConsoleNotifier()
            >>> notifier.notify(opp)
            🔔 OPPORTUNITY | [YES] $0.75 | Market: btc-15min | 14:32:15 (BID)
            True
        """
        try:
            formatted = self._format_opportunity(opportunity)
            print(formatted)
            logger.info(
                "Notified opportunity: %s @ $%.2f (%s) for %s",
                opportunity.side,
                opportunity.price,
                opportunity.source,
                opportunity.market_id,
            )
            return True
        except Exception as e:
            logger.error("Failed to send console notification: %s", e)
            return False

    def notify_batch(self, opportunities: list[Opportunity]) -> int:
        """Print notifications for multiple opportunities.

        Args:
            opportunities: List of opportunities to notify about.

        Returns:
            Number of notifications sent successfully.
        """
        if not opportunities:
            return 0

        # Print header for batch
        count = len(opportunities)
        if self.use_colors:
            header = Colors.colorize(
                f"╔══ {count} Opportunities Detected ══╗",
                Colors.BOLD,
                Colors.MAGENTA,
            )
        else:
            header = f"=== {count} Opportunities Detected ==="

        print(header)

        sent = 0
        for opportunity in opportunities:
            if self.notify(opportunity):
                sent += 1

        # Print footer
        if self.use_colors:
            footer = Colors.colorize(
                "╚" + "═" * (len(f" {count} Opportunities Detected ") + 4) + "╝",
                Colors.BOLD,
                Colors.MAGENTA,
            )
        else:
            footer = "=" * (len(f"=== {count} Opportunities Detected ==="))

        print(footer)

        logger.info("Batch notification complete: %d/%d sent", sent, count)
        return sent

    def notify_window_start(self, window_end: datetime) -> None:
        """Print a notification when entering a monitoring window.

        Args:
            window_end: The end time of the monitoring window.
        """
        timestamp = self._format_timestamp(datetime.now())
        end_time = self._format_timestamp(window_end)

        if self.use_colors:
            status = Colors.colorize("MONITORING", Colors.BOLD, Colors.GREEN)
            msg = f"⏰ {status} | Started at {timestamp} | Window ends at {end_time}"
        else:
            msg = f"[MONITORING] Started at {timestamp} | Window ends at {end_time}"

        print(msg)
        logger.info("Entered monitoring window (ends at %s)", end_time)

    def notify_window_end(self) -> None:
        """Print a notification when the monitoring window ends."""
        timestamp = self._format_timestamp(datetime.now())

        if self.use_colors:
            status = Colors.colorize("WINDOW END", Colors.BOLD, Colors.YELLOW)
            msg = f"⏰ {status} | Monitoring complete at {timestamp}"
        else:
            msg = f"[WINDOW END] Monitoring complete at {timestamp}"

        print(msg)
        logger.info("Exited monitoring window at %s", timestamp)

    def notify_no_opportunities(self) -> None:
        """Print a notification when no opportunities were found in a window."""
        if self.use_colors:
            msg = Colors.colorize(
                "ℹ️  No opportunities detected in this window",
                Colors.DIM,
            )
        else:
            msg = "[INFO] No opportunities detected in this window"

        print(msg)
        logger.info("No opportunities detected in monitoring window")
