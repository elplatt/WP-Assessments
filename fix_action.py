# There was a bug in the parser script that left some "Reassessed" articles blank.
# This script fixes the action column for those entries

import os
import sys

files =  os.listdir("output/assessments")
for i, fname in enumerate(files):
    changed = False
    print "%d/%d: %s" % (i, len(files), fname)
    if fname.split(".")[-1] != "tsv":
        continue
    with open("output/assessments/%s" % fname, "rb") as f_in:
        with open("output/assessments_fixed/%s" % fname, "wb") as f_out:
            # Header
            f_out.write(f_in.next())
            # Iterate through rows
            for row in f_in:
                data = row.split("\t")
                if data[2] == '':
                    data[2] == 'Reassessed'
                    changed = True
                f_out.write("\t".join(data))
    if changed:
        print "  Changed"