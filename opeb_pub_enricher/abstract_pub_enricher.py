#!/usr/bin/python


from abc import abstractmethod

from typing import (
    cast,
    overload,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    import configparser
    from typing import (
        Final,
        Optional,
        Union,
    )

    from .pub_common import (
        EnricherId,
    )

    from .doi_cache import DOIChecker
    from .pub_cache import (
        PubDBCache,
    )

from .skeleton_pub_enricher import SkeletonPubEnricher


class AbstractPubEnricher(SkeletonPubEnricher):
    DEFAULT_REQUEST_DELAY: "Final[float]" = 0.25

    @overload
    def __init__(
        self,
        cache: "str",
        prefix: "Optional[str]" = None,
        config: "Optional[configparser.ConfigParser]" = None,
        doi_checker: "Optional[DOIChecker]" = None,
    ): ...

    @overload
    def __init__(
        self,
        cache: "PubDBCache",
        prefix: "Optional[str]" = None,
        config: "Optional[configparser.ConfigParser]" = None,
        doi_checker: "Optional[DOIChecker]" = None,
    ): ...

    def __init__(
        self,
        cache: "Union[str, PubDBCache]",
        prefix: "Optional[str]" = None,
        config: "Optional[configparser.ConfigParser]" = None,
        doi_checker: "Optional[DOIChecker]" = None,
    ):
        super().__init__(cache, prefix=prefix, config=config, doi_checker=doi_checker)

        # The section name is the symbolic name given to this class
        section_name = self.Name()

        request_delay = self.config.getfloat(
            section_name, "request_delay", fallback=self.DEFAULT_REQUEST_DELAY
        )
        self.request_delay = request_delay

        useragent = self.config.get(
            section_name,
            "useragent",
            fallback="Mozilla/5.0 (X11; Linux x86_64; rv:79.0) Gecko/20100101 Firefox/79.0",
        )
        self.useragent = useragent

    @classmethod
    @abstractmethod
    def Name(cls) -> "EnricherId":
        return cast("EnricherId", "abstract")


if TYPE_CHECKING:
    from typing import (
        TypeVar,
    )

    ImplementedEnricher = TypeVar("ImplementedEnricher", bound=AbstractPubEnricher)
