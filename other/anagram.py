def is_anagram(str1, str2):
    letters = {}

    # Letters occurring in str1 counting
    for letter in str1:
        if not letter in letters:
            letters[letter] = 0

        letters[letter] += 1

    # Letters occurring in str2
    for letter in str2:
        if not letter in letters:
            return False

        letters[letter] -= 1
        if letters[letter] == 0:
            del letters[letter]

    if len(letters.keys()) == 0:
        return True
    else:
        return False


# Tests

print is_anagram('manola', 'alamon')
print is_anagram('jamon', 'mango')
print is_anagram('pepepepe', 'pepepe')

