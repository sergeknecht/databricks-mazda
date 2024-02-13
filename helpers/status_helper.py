import datetime


def create_status(status_message: str = "", status_code: int = 200) -> dict:
    status_timestamp = datetime.datetime.utcnow().isoformat()
    result = {
        "status_code": status_code,
        "status_message": status_message,
        "status_timestamp": status_timestamp,
    }
    return result


if __name__ == "__main__":
    print(create_status())