
class HashCheckLevel:
    BYPASS = 0
    WARNING = 1
    ERROR = 2

    @staticmethod
    def from_string(level_str: str):
        hashcheck_mapping = {
            "BYPASS": HashCheckLevel.BYPASS,
            "WARNING": HashCheckLevel.WARNING,
            "ERROR": HashCheckLevel.ERROR
        }
        return hashcheck_mapping.get(str(level_str).upper(), None)

    @staticmethod
    def to_string(level):
        hashcheck_mapping = {
            HashCheckLevel.BYPASS: "BYPASS",
            HashCheckLevel.WARNING: "WARNING",
            HashCheckLevel.ERROR: "ERROR",
        }
        return hashcheck_mapping.get(level, "Unknown")