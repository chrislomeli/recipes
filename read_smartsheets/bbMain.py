import smartsheet
import sys
import logging
import bbBuilding
import bbLogging
import bbConfig
import bbDataAccess

smartsheet = smartsheet.Smartsheet("Bearer 32q6itwci6q6thvbkxxsp8d5i3")
forceload = False
TESTING=False

"""
---------------------------------------------
get_bases
---------------------------------------------"""
def get_bases():
    Bases = []
    action1 =  smartsheet.Workspaces.list_workspaces()
    workspaces = action1.data
    for ws in workspaces:
        if (ws.name=='Technical Support and Field Operations'):
            action = smartsheet.Workspaces.list_folders(ws.id, include_all=True)
            folders = action.data
            for folder in folders:
                if (folder.name=='Building Out of Service'):
                    folderinfo = smartsheet.Folders.get_folder(folder.id)
                    sheets = folderinfo.sheets
                    print("Using Folder: "+folder.name)
                    for sheetInfo in sheets:
                        sheet = smartsheet.Sheets.get_sheet(sheetInfo.id)
                        columns = smartsheet.Sheets.get_columns(sheetInfo.id)
                        rows =  sheet.rows
                        sys.stdout.write("\t"+sheetInfo.name)
                        for row in rows:
                            building = {}
                            sys.stdout.write('.')
                            sys.stdout.flush()
                            for c in range(0, len(sheet.columns)):
                                cell = row.cells[c]
                                building[columns.data[c].title] =cell.value
                            # end columns
                            buidingObj = bbBuilding.Building(sheetInfo.name, building)
                            if (len(buidingObj.errors) > 0 and not forceload):
                                print("\n\tBase %s - building %s has %d errors" % (buidingObj.baseName, buidingObj.buildingName, len(buidingObj.errors)))
                                for e in buidingObj.errors:
                                    print("\t\t%s (%s)" % (e,buidingObj.toJson()))
                                    sys.exit("error")

                            Bases.append(buidingObj)
                            if TESTING: return Bases
                        # end sheet
                        print("")


                    return Bases

"""
---------------------------------------------
write_json
---------------------------------------------"""
def write_json(bases):

    print("found %d records" % len(bases))

    with open(bbConfig.LOCAL_FILE_NAME, 'w') as fp:
        for rec in bases:
            if rec.fieldcount == 0: continue
            fp.write(rec.toJson())
            fp.write("\n")

        fp.close()
    logging.info("Created file "+bbConfig.LOCAL_FILE_NAME)


"""
---------------------------------------------
main
---------------------------------------------"""
def main():
    #
    global forceload
    if ('-f' == sys.argv[1] ):
        print("option -f is set - this means that any errors will be loaded")
        forceload = True
    bbLogging.setupLogging()

    # get base info from smartsheets
    bases = get_bases()

    # summary
    print("Found %s bases " % len(bases))
    for building in bases:
        if (len(building.errors) > 0 ):
            print("Base %s - building %s has %d errors and %d warnings" % (building.baseName, building.buildingName, len(building.errors), len(building.warnings)))



    # write json to the temp file
    write_json( bases )

    # upload temp file to s3
    logging.info("Upload to S3")
    fn = bbDataAccess.upload_localfile(bbConfig.RED_TABLE_NAME, bbConfig.S3_FILE_NAME, bbConfig.LOCAL_FILE_NAME)
    if (fn != None):
        # copy into redhsift
        logging.info("COPY file %s to Redshift " % fn)
        bbDataAccess.copy_s3file(bbConfig.RED_TABLE_NAME, fn)



if __name__ == "__main__":
    main()