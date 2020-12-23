from datetime import timedelta

def formattimedelta(td):
    """formats a timedelta to ignore the microseconds. (something else down the line calling str() will actually make it a string"""
    return timedelta(seconds=int(td.total_seconds()))