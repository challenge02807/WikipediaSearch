from bz2file import BZ2File
import xml.etree.cElementTree as etree
import sys,time,re

# Output data files
#out_file = open("data/parsed_data_cat",'w+')
#out_file = open("data/parsed_data_a",'w+')
out_file = open("data/parsed_data_all",'w+')

path = "data/enwiki-20170820-pages-articles-multistream.xml.bz2" # Local copy of wiki dump in multistream version
counter = 0 # Used to count number of used articles

#num_files = 1 # Used for cat article
#num_files = 376311.0 # Used for all A-articles
num_files = 5462507.0 # Total number of articles in dump 20170820

start_time = time.time() # Timing the parsing
pattern = re.compile('[\n\r]+') # Pattern to remove newlines

# Use BZ2File to decompress multistream (Non-parallel)
with BZ2File(path) as xml_file:
    parser = etree.iterparse(xml_file, events=('end',)) # Event based parser that triggers on end tag
    print_article = False # Boolean to determine if the article is valid
    id_article = "-1" # The current articles id. Can be validated with: "https://en.wikipedia.org/?curid="+article_id

    for _, elem in parser:
        # Only use article if it has a title

        if "title" in elem.tag:
            #if elem.text != None and re.match(r'^cat$',str.strip(elem.text).lower()):  # USED FOR CAT-ARTICLE (1/2)
            #    print_article = True                                                   # USED FOR CAT-ARTICLE (2/2)

            #if elem.text != None and re.match(r'^[aA]',elem.text):                      # USED FOR A-ARTICLES (1/3)
            #    print_article = True                                                    # USED FOR A-ARTICLES (2/3)
            #    id_article = "-1"                                                       # USED FOR A-ARTICLES (3/3)

            print_article = True                                                       # USED FOR ALL ARTICLES (1/2)
            id_article = "-1"                                                          # USED FOR ALL ARTICLES (2/2)

        if print_article and "id" in elem.tag and id_article == "-1" and elem.text != None:
            id_article = str.strip(elem.text)

        # If redirect tag if found skip article
        if print_article and "redirect" in elem.tag and elem.text == None:
            print_article = False

        # Only keep articles with namespace 0
        if print_article and "ns" in elem.tag and elem.text != None and elem.text != "0":
            print_article = False

        # Only keep articles that does not contain redirects
        if print_article and "text" in elem.tag and "#REDIRECT" not in elem.text[:9].upper():
            # If there is no text skip the article
            if elem.text != None:
                # Print the line to the data file
                print(id_article+":"+pattern.sub(' ', elem.text).lower(), file=out_file)
                counter += 1
                if (num_files == 1): # NOTE: only triggered when searching for Cat article
                    break

            print_article = False

        # Print progress
        if counter % 100 == 0:
            sys.stdout.write("\rParsed %0.2f%% (%i files)" % (counter / num_files * 100,counter))
            sys.stdout.flush()

        # Clear the element so that it wont turn up again
        elem.clear()

    # Print results
    print()
    print("%i files have been parsed in %i seconds" % (counter,time.time() - start_time))