import asyncio

from pynats.aio import Client, ClientOptions


async def main():
    async with Client(
        ClientOptions(
            connect_timeout=10,
            max_connect_attempts=100,
            max_reconnect_attempts=100,
        )
    ) as client:
        try:
            await client.wait_until_closed()
        except KeyboardInterrupt:
            return


if __name__ == "__main__":
    asyncio.run(main())
