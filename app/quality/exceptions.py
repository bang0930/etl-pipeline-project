class DataQualityError(ValueError):
    """파이프라인을 중단해야 하는 데이터 품질 오류."""


class PaginationConsistencyError(DataQualityError):
    """페이지 수집 중 원본 변경이 의심되어 재실행이 필요한 오류."""

    def __init__(self, detail):
        super().__init__(
            f"{detail} "
            "The source data may have changed during pagination. "
            "Retry the same base date."
        )
