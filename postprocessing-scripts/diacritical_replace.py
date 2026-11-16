DIACRITICAL_REPLACEMENTS = {
    'á': 'a',
    'à': 'a',
    'â': 'a',
    'ä': 'a',
    'ã': 'a',
    'å': 'a',
    'ą': 'a',
    'ă': 'a',
    'ā': 'a',
    'ç': 'c',
    'é': 'e',
    'è': 'e',
    'ê': 'e',
    'ë': 'e',
    'í': 'i',
    'ì': 'i',
    'î': 'i',
    'ï': 'i',
    'ñ': 'n',
    'ó': 'o',
    'ò': 'o',
    'ô': 'o',
    'ö': 'o',
    'õ': 'o',
    'ø': 'o',
    'ú': 'u',
    'ù': 'u',
    'û': 'u',
    'ü': 'u',
    'ý': 'y',
    'ÿ': 'y',
    'ł': 'l',
    'Ł': 'l',
    'ž': 'z',
    'ź': 'z',
    'ć': 'c',
    'ę': 'e',
    'ś': 's'
}


def replace_name(value):
    replacement_str = []

    for char in value:
        if char in DIACRITICAL_REPLACEMENTS:
            replacement_str.append(DIACRITICAL_REPLACEMENTS.get(char))
        else:
            replacement_str.append(char)

    return ''.join(replacement_str)