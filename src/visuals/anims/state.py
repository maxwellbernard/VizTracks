"""Animation state container used by bar animations.

Tracks previous and current bar positions, names, and widths to support
smooth transitions between frames.
"""


class AnimationState:
    """Holds per-frame state for bar animations.

    Args:
        top_n: Number of bars being animated.
    """

    def __init__(self, top_n: int) -> None:
        self.prev_interp_positions: list[float]
        self.prev_positions: list[float]
        self.current_new_positions: list[float]
        self.prev_names: list[str]
        self.prev_widths: list[float]

        self.prev_interp_positions = self.prev_positions = (
            self.current_new_positions
        ) = list(range(9, 9 - top_n, -1))
        self.prev_names = [""] * top_n
        self.prev_widths = [0] * top_n
