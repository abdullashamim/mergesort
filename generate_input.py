import datetime, random, string, sys

def generate_random_date():
    today = datetime.datetime.now()
    delta = datetime.timedelta(random.randint(-50, 50))
    d = str(today + delta).split()[0].split("-")
    return d[2] + "/" + d[1] + "/" + d[0]

def generate_random_string(length):
    return "".join(random.choice(string.ascii_lowercase) for x in xrange(length))

if __name__ == "__main__":
    n = int(sys.argv[1])
    for i in xrange(n):
        print "%s,%s,%d,%s" \
                % (generate_random_date(),
                   generate_random_string(11),
                   random.randint(100000, 999999),
                   generate_random_date())
