from zoneinfo import ZoneInfo
from pathlib import Path
import tzlocal


class TimezoneError(Exception):
    pass


def mktz(zone=None):
    """
    Return a new timezone (tzinfo object) based on the zone using the python-dateutil
    package.

    The concise name 'mktz' is for convenient when using it on the
    console.

    Parameters
    ----------
    zone : `String`
           The zone for the timezone. This defaults to local, returning:
           tzlocal.get_localzone()

    Returns
    -------
    An instance of a timezone which implements the tzinfo interface.

    Raises
    - - - - - -
    TimezoneError : Raised if a user inputs a bad timezone name.
    """
    if zone is None:
        return ZoneInfo(tzlocal.get_localzone_name())
    try:
        return ZoneInfo(zone)
    except Exception as e:
        p = Path(zone)
        if p.exists() and p.is_file():
            try:
                with p.open("rb") as fh:
                    return ZoneInfo.from_file(fh)
            except Exception:
                pass
        raise TimezoneError(f'Timezone "{zone}" can not be read') from e
