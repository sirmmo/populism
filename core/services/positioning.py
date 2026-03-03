# core/services/positioning.py

from typing import Dict, List, Optional, Tuple

# Type alias for the positions index:
# key = (party_id, dimension), value = sorted list of (year, value)
PositionsIndex = Dict[Tuple[int, str], List[Tuple[int, float]]]


def pick_value(
    positions: PositionsIndex,
    party_id: int,
    dim: str,
    year: int,
    fill_down: bool = False,
) -> Optional[float]:
    """
    Look up a positioning value for (party, dimension) at a given election year.

    If fill_down is False (default), only return a value if there is an exact
    year match.  If fill_down is True, return the most recent value whose year
    is <= the election year (forward-fill / carry-forward behaviour).
    """
    lst = positions.get((party_id, dim))
    if not lst:
        return None

    if fill_down:
        best_val = None
        for y, v in lst:
            if y <= year:
                best_val = v
            else:
                break  # list is sorted, no need to continue
        return best_val
    else:
        for y, v in lst:
            if y == year:
                return v
        return None
