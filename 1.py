// Сделайте mapper и reducer, чтобы посчитать среднее и дисперсию оценок за фильм.
import json
n, mean, M2 = 0, 0.0, 0
for path in Path('imdb-user-reviews').glob('**/*'):
    if path.is_file() and path.suffix == '.json':
        with open(path, 'r') as f:
            info = json.load(f)
        score = float(info['movieIMDbRating'])
        n += 1
        delta = score - mean
        mean += delta / n
        M2 += delta * (score - mean)

print(mean, (M2 / n) ** (1/2))
def mapper(path):
    # Ваш код
    return (score, )

def reducer(score_data1, score_data2):
    #  Ваш код
    return n, mean, M2
%%time
n, mean, M2 = reduce(reducer, map(mapper, Path('imdb-user-reviews').glob('**/*')))
print(mean, (M2 / n) ** (1/2))