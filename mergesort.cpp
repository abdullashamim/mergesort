#include<cstdio>
#include<memory>
#include<cstring>
#include<string>
#include<iostream>
#include<sstream>
#include<fstream>
#include<cstdlib>
#include<algorithm>
#include<vector>
#include<queue>
#include<unordered_map>
#include<map>
#include<climits>

using namespace std;

#define FASTIO ios_base::sync_with_stdio(0);

// typedef long long int ll;

unordered_map<string, char*> argsMap;
// colName -> (colIndex, colType)
unordered_map<string, pair<int, string> > schema;
int recordSize, RAMSize, nrr, noSplits;
vector<ifstream* > fPointers;
vector<string> sortBy;
vector<int> storeCols;
bool iorder;

/* Helper function, splits a comma separated string */

vector<string> &split(const string &s, char delim, vector<string> &elems) {
    stringstream ss(s);
    string item;
    while (getline(ss, item, delim)) {
        elems.push_back(item);
    }
    return elems;
}

vector<string> split(const string &s, char delim) {
    vector<string> elems;
    split(s, delim, elems);
    return elems;
}

bool colCompare(const vector<string>& r1, const vector<string>& r2, int colIdx, bool heap) {

    if (colIdx >= sortBy.size()) {
        // sure ?
        return false;
    }

    bool ret;
    string val1 = r1[schema[sortBy[colIdx]].first];
    string val2 = r2[schema[sortBy[colIdx]].first];
    string type = schema[sortBy[colIdx]].second;

    if (val1 == val2) {
        return colCompare(r1, r2, colIdx + 1, heap);
    }

    if (type == "date") {
        // sort by date
        vector<string> date1 = split(val1, '/');
        vector<string> date2 = split(val2, '/');
        for(int i=date1.size()-1; i>=0; i--) {
            if (date1[i] == date2[i]) {
                continue;
            } else {
                if (iorder) {
                    ret = ((date1[i] < date2[i]) ? true : false);
                } else {
                    ret = ((date1[i] > date2[i]) ? true : false);
                }
                break;
            }
        }
        return (heap ? !ret : ret);

    } else {
        // sort by char(..) or number
        if (iorder) {
            ret = ((val1 < val2) ? true : false);
            // heap is opposite
            return (heap ? !ret : ret);
        } else {
            ret = ((val1 > val2) ? true : false);
            return (heap ? !ret : ret);
        }
    }
}

bool compareRecords(const vector<string>& r1, const vector<string>& r2) {
    return colCompare(r1, r2, 0, false);
}

bool heapCompare(const pair<vector<string>, int>& p1, const pair<vector<string>, int>& p2) {
    return colCompare(p1.first, p2.first, 0, true);
}

priority_queue<pair<vector<string>, int>, vector<pair<vector<string>, int> >, decltype(&heapCompare)> myheap(&heapCompare);

void parseArgs(int noArgs, char *args[]) {

    string key;
    bool keyFound;
    string value;

    for(int i=0; i<noArgs; i++) {
        if (args[i][0] == '-') {
            // assume --something
            // reset key
            key = "";
            for(int j=2; args[i][j]!='\0'; j++) {
                key += args[i][j];
            }
            keyFound = true;
        } else if (keyFound) {
            keyFound = false;
            argsMap[key] = args[i];
        }
    }

    sortBy = split(string(argsMap["sort_column"]), ',');
    iorder = string(argsMap["order"]) == "asc" ? true : false;
}

int getLength(string s) {
    if (s == "int") {
        return 4;
    } else if (s == "date") {
        return 10;
    } else {
        int flag = 0;
        string no;
        for(int i=0; i<s.length(); i++) {
            if (s[i] == '(') {
                flag = 1;
                continue;
            }
            if (s[i] == ')') {
                flag = 0;
                continue;
            }
            if (flag) {
                no += s[i];
            }
        }
        return atoi(no.c_str());
    }
}

void readMetaFile() {

    int colIndex = 0;
    char ch;
    FILE *f;
    f = fopen(argsMap["meta_file"], "r");
    string colName, colType;
    colName = colType = "";
    int icolSize;
    bool colNameFlag = true;
    while((ch = fgetc(f)) != EOF) {
        if (ch == '\n') {

            // store the col into col dict
            schema[colName] = make_pair(colIndex, colType);
            recordSize += getLength(colType);

            // reset params
            colName = "";
            colType = "";
            colNameFlag = true;
            // increment number of cols
            colIndex++;
            continue;
        }
        if (ch == ',') {
            colNameFlag = false;
            continue;
        }
        if (colNameFlag) {
            colName += ch;
        } else {
            colType += ch;
        }
    }
    fclose(f);
}

void splitIntoFiles(int nrr) {

    string line, tempR;
    ofstream tempF;
    ifstream f(argsMap["input_file"]);
    // read nrr records from file and put into split files
    // https://gehrcke.de/2011/06/reading-files-in-c-using-ifstream-dealing-correctly-with-badbit-failbit-eofbit-and-perror/
    int cnt = 0;
    vector<vector<string> > records;
    vector<string> record;
    while(getline(f, line)) {
        cnt++;
        // split record by ,
        record = split(line, ',');
        records.push_back(record);
        if (cnt == nrr) {
            // that's it, enough records in one block
            // now sort
            sort(records.begin(), records.end(), compareRecords);
            /*
            for(int i=0; i<records.size(); i++) {
                for(int j=0; j<records[i].size(); j++) {
                    cout << records[i][j] << ",";
               }
                cout << endl;
            }*/
            cnt = 0;
            noSplits++;
            tempF.open(("dump/file" + to_string(noSplits)).c_str());
            for(int i=0; i<records.size(); i++) {
                tempR = records[i][0];
                for(int j=1; j<records[i].size(); j++) {
                    tempR += "," + records[i][j];
                }
                tempR += "\n";
                tempF << tempR;
            }
            tempF.close();
            records.erase(records.begin(), records.end());
        }
    }
    if (cnt < nrr) {
        // that's it, enough records in one block
        // now sort
        sort(records.begin(), records.end(), compareRecords);
        /*
        for(int i=0; i<records.size(); i++) {
            for(int j=0; j<records[i].size(); j++) {
                cout << records[i][j] << ",";
            }
            cout << endl;
        }*/
        cnt = 0;
        noSplits++;
        tempF.open(("dump/file" + to_string(noSplits)).c_str());
        for(int i=0; i<records.size(); i++) {
            tempR = records[i][0];
            for(int j=1; j<records[i].size(); j++) {
                tempR += "," + records[i][j];
            }
            tempR += "\n";
            tempF << tempR;
        }
        tempF.close();
        records.erase(records.begin(), records.end());
    }
}

void mergeFiles(int num) {

    // save output buffer
    // Assume output buffer size
    // Output buffer takes 20% of RAM
    int OB = .2 * (double)RAMSize;
    // number of records that fit in Output Buffer
    int nOB = OB/recordSize;
    // number of records from each file
    int nF = ((double)((RAMSize-OB)/noSplits))/recordSize;
  
    // Store all file pointers in an array
    for(int i=0; i<noSplits; i++) {
        ifstream *file = new ifstream(("dump/file" + to_string(i+1)).c_str());
        fPointers.push_back(file);
    }

    ifstream *f;
    string line;
    vector<string> record;
    int cnt;
   // cout << nF << endl;

    // put necessary records into heap
    for(int i=0; i<noSplits; i++) {
        f = fPointers[i];
        cnt = 0;
     //   cout << "Reading from one file!" << endl;
        while(getline(*f, line)) {
            cnt++;
            record = split(line, ',');
            myheap.push(make_pair(record, i));
            if (cnt == nF) {
                break;
            }
        }
    }
    
    // count of records in output buffer
    int cntOB = 0;
    ofstream outFile;
    outFile.open(argsMap["output_file"]);
    string tempR;
    pair<vector<string>, int> ele;
    vector<vector<string> > mainRecords;

    // Now keep putting records into output buffer till full, then repeat
    while(!myheap.empty()) {
        ele = myheap.top();
        myheap.pop();
        // push record to output buffer
        mainRecords.push_back(ele.first);
        // add new record from filepointer to heap
        if (getline(*fPointers[ele.second], line)) {
            record = split(line, ',');
            myheap.push(make_pair(record, ele.second));
        } else {
            // ifstream is empty,
            // do nothing(?)
        }
        cntOB++;
        if (cntOB == nOB) {
            // output buffer is full
            // put records to file
            for(int i=0; i<mainRecords.size(); i++) {
                // only put specific columns into the file
                tempR = mainRecords[i][storeCols[0]];
                for(int j=1; j<storeCols.size(); j++) {
                    tempR += "," + mainRecords[i][storeCols[j]];
                }
                tempR += "\n";
                outFile << tempR;
            }
            // clear Buffer
            cntOB = 0;
            mainRecords.erase(mainRecords.begin(), mainRecords.end());
        }   
    }

    // send leftovers
    if (cntOB < nOB) {
        // output buffer is full
        // put records to file
        for(int i=0; i<mainRecords.size(); i++) {
            // only put specific columns into the file
            tempR = mainRecords[i][storeCols[0]];
            for(int j=1; j<storeCols.size(); j++) {
                tempR += "," + mainRecords[i][storeCols[j]];
            }
            tempR += "\n";
            outFile << tempR;
        }
        // clear Buffer
        cntOB = 0;
        mainRecords.erase(mainRecords.begin(), mainRecords.end());
    }

    // Cleanup
    // delete dynamic pointers
    for(int i=0; i<noSplits; i++) {
        delete fPointers[i];
    }
    fPointers.erase(fPointers.begin(), fPointers.end());
    // close output file
    outFile.close();
}

int main(int noArgs, char *args[]) {

    FASTIO;
    // parse command line arguments
    parseArgs(noArgs, args);
    // Read column names, and size
    readMetaFile();

    // get output cols index
    vector<string> outputCols = split(string(argsMap["output_column"]), ',');
    for(int i=0; i<outputCols.size(); i++) {
        storeCols.push_back(schema[outputCols[i]].first);
    }

    // in Bytes lower bound
    // --mm always integer ?
    RAMSize = (int)((double)(atoi(argsMap["mm"]) * 1024 * 1024) * 0.8);
    // number of records that can be held in one block or buffer(RAM)
    nrr = RAMSize/recordSize;
   // cout << nrr << endl;
    splitIntoFiles(nrr);

    mergeFiles(nrr);
    return 0;
}
