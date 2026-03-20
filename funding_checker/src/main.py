import asyncio

from volume_tracker import run_volume_tracker
from hedge_strategy import run_hedge_strategy
from trading_tools import run_trading_tools
from unique_strategy import run_unique_strategy
from funding_monitor import run_funding_monitor

async def main():
    await asyncio.gather(
        run_hedge_strategy(),
        run_volume_tracker(),
        run_trading_tools(),
        run_unique_strategy(),
        run_funding_monitor()
    )

if __name__ == "__main__":
    asyncio.run(main())