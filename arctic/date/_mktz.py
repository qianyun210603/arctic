import dateutil
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
        # tzlocal provides get_localzone_name() in newer versions; fall back to examining the tzinfo
        try:
            zone = tzlocal.get_localzone_name()
        except Exception:
            local_tz = tzlocal.get_localzone()
            # Try common attributes that may contain the zone name
            zone = getattr(local_tz, 'zone', None) or getattr(local_tz, 'key', None) or str(local_tz)

    tz = dateutil.tz.gettz(zone)
    if not tz:
        raise TimezoneError(f'Timezone "{zone}" can not be read')
    # Stash the zone name as an attribute (as pytz does)
    if not hasattr(tz, 'zone'):
        try:
            tz.zone = zone
        except Exception:
            # Some tzinfo implementations may not allow setting attributes; ignore in that case
            pass
        else:
            # Try to set a filename attribute used by some pandas/dateutil code paths
            # (e.g. pandas may look for tz._filename for dateutil tz objects). Be
            # defensive: only set when `zone` is a string and when the tz object
            # allows attribute assignment.
            try:
                if isinstance(zone, str):
                    # Prefer a shorter filename when the zone string includes one of
                    # dateutil.tz.TZPATHS as a prefix (matches pandas/dateutil behaviour).
                    for p in getattr(dateutil.tz, 'TZPATHS', []):
                        if zone.startswith(p):
                            try:
                                tz._filename = zone[len(p) + 1:]
                            except Exception:
                                tz._filename = zone
                            break
                    else:
                        # No TZPATHS prefix matched — store the raw zone string.
                        tz._filename = zone
            except Exception:
                # If any attribute assignment fails, ignore — we do not want to
                # raise from this best-effort metadata addition.
                pass
                if zone.startswith(p):
                    try:
                        tz.zone = zone[len(p) + 1:]
                    except Exception:
                        pass
    return tz
