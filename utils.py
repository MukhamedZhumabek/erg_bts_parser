
def check_text_match_all_keys(text, keys):
    for key in keys:
        if key not in text:
            return False
    return True