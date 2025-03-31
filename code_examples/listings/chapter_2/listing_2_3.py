@asset(schedule=raw_zen_quotes)  # A
def selected_quotes(raw_zen_quotes):  # B
    """
    Transforms the extracted raw_zen_quotes.
    """

    import numpy as np  # C

    quotes_character_counts = [
        int(quote["c"]) for quote in raw_zen_quotes
    ]
    median = np.median(quotes_character_counts)

    median_quote = min(
        raw_zen_quotes,
        key=lambda quote: abs(int(quote["c"]) - median),
    )
    raw_zen_quotes.pop(
        raw_zen_quotes.index(median_quote)
    )
    short_quote = [
        quote
        for quote in raw_zen_quotes
        if int(quote["c"]) < median
    ][0]
    long_quote = [
        quote
        for quote in raw_zen_quotes
        if int(quote["c"]) > median
    ][0]

    return {
        "median_q": median_quote,
        "short_q": short_quote,
        "long_q": long_quote,
    }  # D


# A Materialize as soon as the raw_zen_quotes asset has received an update, this is a data-aware schedule
# B Fetch the raw quotes from the upstream asset
# C Import numpy at runtime to calculate the median length of quotes
# D Multiple values can be returned in a dictionary to be stored as XComs
