import json
import typing
import argparse
from datetime import datetime
from pathlib import Path
from functools import wraps
import time


LOG_FILENAME = 'merged_log.jsonl'
DATE_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'


def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f'Function {func.__name__} Took {total_time:.4f} seconds')
        return result
    return timeit_wrapper


def open_file_generator(log_file: Path) -> typing.Generator[dict, None, None]:
    with open(log_file, 'rb') as file:
        for line in file:
            yield json.loads(line)


def next_log(log_from_file_generator: typing.Generator) -> typing.Union[dict, None]:
    try:
        return next(log_from_file_generator)
    except StopIteration:
        return None


def get_log_time(log_instance: dict) -> datetime:
    return datetime.strptime(log_instance.get('timestamp'), DATE_TIME_FORMAT)


def sorted_log_generator(log1_path: Path, log2_path: Path) -> typing.Generator[dict, None, None]:
    """Compare timestamps of logs. Returns the log which is younger."""
    file_log1_generator = open_file_generator(log1_path)
    file_log2_generator = open_file_generator(log2_path)
    file_log1 = next_log(file_log1_generator)
    file_log2 = next_log(file_log2_generator)

    while file_log1 or file_log2:
        if file_log1 and file_log2:
            if get_log_time(file_log1) <= get_log_time(file_log2):
                yield file_log1
                file_log1 = next_log(file_log1_generator)
            else:
                yield file_log2
                file_log2 = next_log(file_log2_generator)
        elif not file_log1:
            yield file_log2
            file_log2 = next_log(file_log2_generator)
        elif not file_log2:
            yield file_log1
            file_log1 = next_log(file_log1_generator)


def create_output_file(output_dir: Path, merged_log_generator: typing.Generator[dict, None, None]) -> None:
        if output_dir.exists():
            raise FileExistsError(f'Folder "{output_dir}" already exists.')
        output_dir.mkdir(parents=True)
        
        output_file_path = output_dir / LOG_FILENAME
        with output_file_path.open('wb') as file:
            for log_entry in merged_log_generator:
                line = json.dumps(log_entry).encode('utf-8')
                line += b'\n'
                file.write(line)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        dest='path_to_log1',
        metavar='<path/to/log1>',
        type=str,
        help='path to the first input file',
    )
    parser.add_argument(
        dest='path_to_log2',
        metavar='<path/to/log2>',
        type=str,
        help='path to second input file',
    )
    parser.add_argument(
        '-o',
        dest='path_to_merged_log',
        required=True,
        metavar='<path/to/merged/log>',
        type=str,
        help='path to the output dir',
    )

    return parser.parse_args()

@timeit
def main() -> None:
    args = parse_args()
    log_path1 = Path(args.path_to_log1)
    log_path2 = Path(args.path_to_log2)
    output_dir = Path(args.path_to_merged_log)

    if log_path1.is_file() and log_path2.is_file():
        merged_log_generator = sorted_log_generator(log_path1, log_path2)
        create_output_file(output_dir, merged_log_generator)
    else:
        raise FileExistsError('Log files do not exist or paths are incorrect.')


if __name__ == '__main__':
    main()