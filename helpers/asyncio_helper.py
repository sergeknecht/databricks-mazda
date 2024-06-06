import asyncio
from typing import Any, Awaitable


async def run_sequence(*functions: Awaitable[Any]) -> Any:
    results = []
    for function in functions:
        results.append(await function)

    return results


async def run_parallel(*functions: Awaitable[Any]) -> Any:
    return await asyncio.gather(*functions)


# # Example usage of run_sequence and run_parallel witjout output
# async def main() -> None:
#     await run_sequence(
#         asyncio.sleep(1),
#         asyncio.sleep(2),
#         asyncio.sleep(3),
#     )

#     await run_parallel(
#         asyncio.sleep(1),
#         asyncio.sleep(2),
#         asyncio.sleep(3),
#     )

#     # example run_sequence with unpacking a list
#     await run_sequence(*[asyncio.sleep(i) for i in range(1, 4)])


# # https://www.youtube.com/watch?v=2IW-ZEui4h4
# # https://github.com/ArjanCodes/2021-asyncio/blob/main/after-2/main.py

# # example with output from run_sequence and run_parallel
# async def do_task() -> list:
#     results = []

#     work_items = run_sequence(
#         wait_and_return_msg(Message("0", MessageType.SWITCH_ON, "run_sequence")),
#         wait_and_return_msg(Message("1", MessageType.SWITCH_OFF, "run_sequence")),
#         wait_and_return_msg(Message("2", MessageType.CHANGE_COLOR, "run_sequence")),
#     )
#     # get result from work_items and append to results
#     results.extend(await work_items)

#     return_values = run_parallel(
#         wait_and_return_msg(Message("3", MessageType.PLAY_SONG, "run_parallel")),
#         wait_and_return_msg(Message("4", MessageType.OPEN, "run_parallel")),
#         wait_and_return_msg(Message("5", MessageType.CLOSE, "run_parallel")),
#     )
#     results.extend(await return_values)

#     # example run_sequence with unpacking a list
#     results.extend(
#         await run_sequence(
#             *[
#                 wait_and_return_msg(Message(str(i), MessageType.CLOSE, "run_sequence"))
#                 for i in range(6, 9)
#             ]
#         )
#     )

#     pp.pprint(results, width=100, indent=4)

#     return results


# if __name__ == "__main__":
#     asyncio.run(do_task())
