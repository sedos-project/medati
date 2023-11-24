from pybtex.database.input import bibtex


def read_bibtex(path):
    parser = bibtex.Parser()
    sources = parser.parse_file(path)
    return sources

def read_bibTexKeys(path):
    bibtexKeys = []
    # read in bib tex keys as list (code needs to be altered for dicts and df´s to be processed)
    return bibtexKeys

def extract_sources(bibTexKeys, sources):
    sourcesList = {}
    for source in bibTexKeys:
      if source not in list(sources.entries._keys.values()):
                  print('Source ' + source + ' not found in sources list.')
                  newSource = dict()
                  newSource["title"] = source
                  newSource["description"] = ""
                  newSource["path"] = ""
                  newSource["licenses"] = []
              else:
                  newSource = dict()
                  if 'subtitle' in sources.entries[source].fields:
                      if not sources.entries[source].fields['subtitle'].empty:
                          newSource["title"] = sources.entries[source].fields['title'] + ' - ' + sources.entries[source].fields['subtitle']
                  else:
                      newSource["title"] = sources.entries[source].fields['title']
                  if 'abstract' in sources.entries[source].fields:
                      newSource["description"] = sources.entries[source].fields['abstract']
                  else:
                      newSource["description"] = ''
                  if 'url' in sources.entries[source].fields:
                      newSource["path"] = sources.entries[source].fields['url']
                  else:
                      newSource["path"] = ''
                  if 'licenses_name' in sources.entries[source].fields:
                      newSource["licenses"] = [
                          {"name": sources.entries[source].fields['licenses_name'],
                          "title": sources.entries[source].fields['licenses_title'],
                          "path": sources.entries[source].fields['licenses_path'],
                          "instruction": sources.entries[source].fields['licenses_instruction'],
                          "attribution": sources.entries[source].fields['licenses_attribution'] + ' ' + sources.entries[source].fields['author'],
                          }
                      ]
                  else:
                      newSource["licenses"] = []
        sourcesList.append(newSource)


if __name__ == "__main__":
    sources = read_bibtex('O:/ESY/06_Projekte-ST/BMWi SEDOS/06_Literatur/20231018_SEDOS.bib')
    bibTexKeys = read_bibTexKeys()
    extract_sources(bibTexKeys, sources)
