import os,re,time,regex
from itertools import chain
from multiprocessing import Pool
from multiprocessing import cpu_count
from ast import literal_eval

def parse_to_func(orig_pattern):
    str_out = []
    for char in orig_pattern:
        if char == "[":
            str_out.append(" ")
            str_out.append("(")
        elif char == "]":
            str_out.append(")")
            str_out.append(" ")
        else:
            str_out.append(char)

    out = []
    for elem in ''.join(str_out).split():
        if re.match(r'\(\d+,\d+\)',elem):
            out.append(literal_eval(elem))
        else:
            out.append(elem)
    return out

def get_matches_starting_at(data, pattern, index, starting_index, resulting_matches):
    sub_data = data[index:]

    P = pattern[0]
    if sub_data[:len(P)] == P:
        if len(pattern) == 1:
            resulting_matches.append((starting_index, index + len(P)))
        else:
            for new_index in range(pattern[1][0] + 1, pattern[1][1] + 2):
                get_matches_starting_at(data, pattern[2:], index + len(P) + new_index - 1, starting_index,
                                        resulting_matches)

        return resulting_matches
    else:
        return []


def get_all_matches(data, pattern):
    matches_found = []
    for index in range(len(data)):
        matches_at_index = get_matches_starting_at(data, pattern, index, index, [])
        matches_found += matches_at_index

    return set(matches_found)

# Inital function for retrieving matches. Calls recursive function below.
def find_all_matches(orig_pattern, data):
    matches = re.findall(r'(?=(' + orig_pattern + '))', data)  # Find all overlapping matches
    out = []  # Collecting list
    if len(matches) > 0:
        for m in matches:
            out.append(find_nested_matches(orig_pattern, m, []))  # Call recursive function to get nested substrings
        return out

# Recursive function to retrieve nested matches
def find_nested_matches(orig_pattern, data, app):
    matches = regex.finditer(orig_pattern, data, concurrent=True)  # Find largest match
    for m in matches:
        app.append(m.group(0))  # Save match
        tmp = data[:(m.start() + len(m.group(0)) - 1)]  # Cut the string to search for next largest match
        return find_nested_matches(orig_pattern, tmp, app)  # Recursive call
    return app

# Define function that is run as a process by itself
def searcher(id,cores,path,round_size,orig_pattern):

    # Translate the pattern to a regular expression for python
    sub_dict = {"[":".{" , "]": "}"}
    pattern = re.compile("(%s)" % "|".join(map(re.escape, sub_dict.keys())))
    short_pattern = pattern.sub(lambda str: sub_dict[str.string[str.start():str.end()]], orig_pattern).lower()

    # Each process opens own filehandle. Read bytes.
    filehandle = open(path, 'rb')

    # Find each process' start byte
    start = 0
    if id != 0: # First process starts from byte 0
        filehandle.seek(id * rounded_size,0) # Find approximate start point
        filehandle.readline() # Go to next newline to avoid cutting a sentence
        start = filehandle.tell() # Get the index of newline byte

    # Find each process' end byte
    end = file_size
    if id != (cores-1): # Last process ends at byte equal to file size
        filehandle.seek((id + 1) * rounded_size,0) #Find approximate end (the next process' start)
        filehandle.readline() # Go to next newline
        end = filehandle.tell() # Get end bytes index

    # Not enough lines causes processes to have the same start and end bytes
    # In this case, terminate the process. First process will do the search.
    if end == start:
        return []

    # Search beginning from the process' start byte
    filehandle.seek(start,0)

    out = []
    while True:
        # Split the line to get article id and text
        article_id , line = re.search('(^\d+):(.+$)', filehandle.readline().decode("utf-8")).groups()

        # Use multithreading regex module to probe article for matches
        matches = regex.findall(short_pattern, line, concurrent=True)

        if len(matches) > 0: # Found at least one match
            """
            match = find_all_matches(r''+short_pattern+'',line) # Find all nested matches
            if match != None:
                match = set(list(chain.from_iterable(match)))  # Flatten list of matches and uniq them
                for m in match:
                    out.append((article_id,m)) # Return tuple of article id and match
            """
            match = get_all_matches(line, parse_to_func(orig_pattern))
            for m in match:
                #print(line[m[0]:m[1]])
                out.append(line[m[0]:m[1]])
        # Break loop to terminate process when hitting the end byte
        if filehandle.tell() == end:
            break

    return out # Terminate

def list_arg_to_str(pat_list):
    pattern_mod = ""
    for elem in pat_list:
        pattern_mod += str.replace(str.replace(str.replace(str(elem), '(', '['), ')', ']'), ' ', '')
    return pattern_mod

def format_results(res):
    match_counter = 0
    out_str = ""
    for tmp_list in res:
        for match in tmp_list:
            match_counter += 1
            #out_str += match[0] + "\t" + match[1] + "\n"
            out_str += match+"\n"
    return (str(match_counter) + "\n" + out_str)

if __name__ == '__main__':

    start_time = time.time() #Start timing
    pattern = list_arg_to_str(['dogs', (0, 15), 'are',(0,15),"to"]) # Translate pattern

    # Count the number of CPUs (incl. virtual CPUs due to hyperthreading)
    # Multiply by a scale (3 seems to be a alright)
    cores = cpu_count() * 3
    file_path = "data/parsed_data_all"
    file_size = os.path.getsize(file_path)
    rounded_size = int(file_size/cores) # Approximate bytes that each process should handle

    # Loop to pass arguments to each process
    # Yield 2D list
    args = []
    for i in range(cores):
        inner = []
        inner.append(i) # Process id
        inner.append(cores) # Number of processes
        inner.append(file_path) # Path to parsed data file
        inner.append(rounded_size)
        inner.append(pattern) # The altered pattern
        args.append(inner)

    # Create a pool of processes, map all args to each process and run!
    with Pool(processes=cores) as pool:
        results = pool.starmap(searcher, args)
        print(format_results(results))
        print()
        print("(Query took %i seconds in real time)" % (time.time() - start_time))