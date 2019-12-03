import asyncio
import concurrent.futures
import logging
from dataclasses import dataclass

logger = logging.getLogger("exec")


@dataclass()
class PrioritizedItem:
    process: asyncio.subprocess.Process
    cmd: list

    def __lt__(self, other):
        return self.cmd < other.cmd


async def run(
    executable, cmd, base_priority=10000, completion=None, output=None, queue=None
):
    logger.debug("Running %s with %s" % (executable, cmd))
    process = None
    try:
        process = await asyncio.create_subprocess_exec(
            executable,
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        if completion:
            if output:
                while process.returncode is None:
                    await output(process, cmd)
            stdout, stderr = await process.communicate()
            logger.debug(f"{cmd} terminated")
            if asyncio.iscoroutinefunction(completion):
                await completion(process, cmd, stderr, stdout)
            else:
                completion(process, cmd, stderr, stdout)
        elif queue:
            queue.put_nowait((base_priority, PrioritizedItem(process, cmd)))
    except (asyncio.CancelledError, concurrent.futures.CancelledError):
        if process:
            process.terminate()
        raise


async def sync_run(executable, cmd):
    process = None
    try:
        process = await asyncio.create_subprocess_exec(
            executable,
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        logger.debug(f"{cmd} terminated")
        return stdout, stderr
    except asyncio.CancelledError:
        if process:
            process.terminate()
        raise


async def run_simultaneously(commands, max_concurrency=None, queue=None):
    if isinstance(max_concurrency, int):
        tasks, results = set(), list()
        for i in range(len(commands)):
            if len(tasks) >= max_concurrency:
                _done, tasks = await asyncio.wait(
                    tasks, return_when=asyncio.FIRST_COMPLETED
                )
            executable, cmd = commands[i]
            tasks.add(
                asyncio.create_task(
                    run(
                        executable,
                        cmd,
                        completion=lambda _, __, ___, stdout: results.append(stdout)
                        if stdout
                        else None,
                        queue=queue,
                    )
                )
            )
        await asyncio.wait(tasks)
        return results
    else:
        tasks = [
            asyncio.create_task(run(executable, cmd, queue=queue))
            for executable, cmd in commands
        ]
        await asyncio.gather(*tasks)
        return tasks


async def exec_sql(
    db_options, script=None, command=None, sync=False, wrap_json=True, completion=None
):
    if not script and not command:
        raise ValueError("Must specify a script or command to execute")

    formatting = ["--tuples-only", "--no-align"]
    task = (
        ["-f", str(script)]
        if script
        else [
            "-c",
            (
                f"SELECT json_agg(query) FROM ({command}) query;"
                if wrap_json
                else command
            ),
        ]
    )

    cmd = [
        "psql",
        (
            [" ".join([f"{k}={v}" for k, v in db_options.items()])]
            if len(db_options) > 0
            else []
        )
        + formatting
        + task,
    ]
    logger.debug(cmd)
    if not sync:
        await run(*cmd, completion=completion)
    else:
        return await sync_run(*cmd)
