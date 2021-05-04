

The criteria:

1. Top N most common terms for the following languages:

top_words_ref = dict(eng=2300000, ger=1350000,
                     fre=900000, lat=900000, rus=900000,
		                          jpn=750000, ita=750000, spa=750000)

2. Top N for the other languages, where N is the greater of 40k or 8% of vocab

That resulted in 15m tokens, 8.9 after duplicates removed.

3. Nearly 1m tokens were removed based on the following (fairly conservative) rules:

   junk = wordlist[~hyphenated & ~alphaadv & (tlen >= 2) & ~endwithperiod & ~singlequote & ~number & ~abbr & ~blankchar]

where that selection criteria was:

tokens = wordlist.index
hyphenated = tokens.str.contains(r"-")
alphaadv = tokens.map(lambda x: not not regex.search("^\\w+$", x))
number = tokens.str.contains(r"^(£|$|€)?[\d.,]+(st|nd|rd|th|s|C|F|c|m|°|¥)?$")
singlequote = tokens.str.contains(r"[\'’]")
abbr = tokens.str.contains(r"^[^\W\d]([^\W\d]|\.)+$")
endwithperiod = tokens.str.endswith('.')
# This shows up for many asian characters, should be dealt with *before* wordlist is created
blankchar = (tokens.str.startswith('\u200b') | tokens.str.endswith("\u200b"))
tlen = tokens.str.len()
