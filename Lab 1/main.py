import csv
import random
import statistics
import concurrent.futures
from pathlib import Path


letters = ["A", "B", "C", "D"]
num_files = 5
rows_per_file = 50

Path("data").mkdir(exist_ok=True)

for i in range(1, num_files + 1):
    filename = Path("data") / f"file_{i}.csv"
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Категория", "Значение"])
        for _ in range(rows_per_file):
            writer.writerow([random.choice(letters), round(random.uniform(1, 100), 2)])


def process_file(filename):
    data = {letter: [] for letter in letters}
    with open(filename, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            data[row["Категория"]].append(float(row["Значение"]))
    return data


all_data = {letter: [] for letter in letters}

with concurrent.futures.ThreadPoolExecutor() as executor:
    files = list(Path("data").glob("*.csv"))
    results = executor.map(process_file, files)
    for file_data in results:
        for letter in letters:
            all_data[letter].extend(file_data[letter])

stats = {}
for letter in letters:
    values = all_data[letter]
    if values:
        median_val = statistics.median(values)
        stdev_val = statistics.pstdev(values)
        stats[letter] = (median_val, stdev_val)
    else:
        stats[letter] = (None, None)

print("Результаты по категориям:")
for letter, (median_val, stdev_val) in stats.items():
    print(f"{letter}: медиана={median_val}, отклонение={stdev_val}")


medians = [m for m, _ in stats.values() if m is not None]

median_of_medians = statistics.median(medians)
stdev_of_medians = statistics.pstdev(medians)

print("\nИтоговые показатели:")
for letter in letters:
    print(f"{letter}: медиана медиан={median_of_medians}, отклонение медиан={stdev_of_medians}")