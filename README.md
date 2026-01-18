This repository contains a Python-based program that implements a trading bot for Polymarket.

Specifically, we will be trading on ONLY the short-term (15-minute) crypto markets, configurable by the user.

The general workflow is:
- 5 minutes into the "current" market (at the 5th, 20th, 35, or 50th minute of the hour) we will redeem any winnings for positions in resolved markets
- Starting 3 minutes before the end of the "current" market, we will open a websocket connection for the order book.
- During the final 3 minutes of trading, if the bid or last trade price for either side goes above a user-configurable value (by default $0.70), we will place a limit order for a user-configurable number of shares (by default 3) for that side.  

Safeguards:
- The "current" market is defined as the 15-minute market window that matches the current real time.
- We should not place trades if the account balance drops below a certain 

