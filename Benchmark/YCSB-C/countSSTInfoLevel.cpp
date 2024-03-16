#include <bits/stdc++.h>

using namespace std;

vector<string> split(string str, string token) {
    vector<string> result;
    while (str.size()) {
        size_t index = str.find(token);
        if (index != std::string::npos) {
            result.push_back(str.substr(0, index));
            str = str.substr(index + token.size());
            if (str.size() == 0) result.push_back(str);
        } else {
            result.push_back(str);
            str = "";
        }
    }
    return result;
}

bool lineProcess(string currentline, int &dataType, int &keySize,
                 int &dataSize) {
    vector<string> currentLineContentVec = split(currentline, " ");
    dataType = stoi(
        currentLineContentVec[2].substr(currentLineContentVec[2].find(":") + 1));
    keySize = (currentLineContentVec[0].length() - 2) / 2;
    dataSize = currentLineContentVec[4].length() / 2;
    return true;
}

enum dataTypeSet { VALUE,
                   DELTA,
                   POSITION };

bool readManifestLog(ifstream &inputStream, unordered_map<int, vector<int>> &levelIDtoSSTIDMap) {
    string currentLine;
    while (getline(inputStream, currentLine)) {
        if (currentLine.find("--- level") != string::npos) {
        newLevel:
            vector<string> tempStrVec = split(currentLine, " ");
            int levelID = stoi(tempStrVec[2]);
            cerr << "Process level ID = " << levelID << endl;
            vector<int> sstIDVec;
            while (getline(inputStream, currentLine)) {
                if (currentLine.find("--- level") != string::npos) {
                    if (sstIDVec.size() == 0) {
                        cerr << "Skip empty level" << endl;
                        goto newLevel;
                    } else {
                        levelIDtoSSTIDMap.insert(make_pair(levelID, sstIDVec));
                        goto newLevel;
                    }

                } else if (currentLine.find(":") == string::npos) {
                    break;
                }
                vector<string> tempLineVec = split(currentLine, ":");
                int sstID = stoi(tempLineVec[0]);
                cerr << "In level " << levelID << " find SSTable ID = " << sstID << endl;
                sstIDVec.push_back(sstID);
            }
        }
    }
    return true;
}

bool readSSTInfoLog(ifstream &inputStream, unordered_map<int, vector<uint64_t>> &SSTIDtoSSTInfoMap) {
    string currentLine;
    while (getline(inputStream, currentLine)) {
        if (currentLine.find("SST ID") != string::npos) {
            vector<string> tempSSTIDVec = split(currentLine, " ");
            int SSTID = stoi(tempSSTIDVec[3]);
            cerr << "Process SST ID = " << SSTID << endl;
            vector<uint64_t> sstInfoVec;
            for (int i = 0; i < 3; i++) {
                getline(inputStream, currentLine);
                for (int j = 0; j < 3; j++) {
                    getline(inputStream, currentLine);
                    vector<string> tempLineVec = split(currentLine, " ");
                    sstInfoVec.push_back(stoull(tempLineVec[tempLineVec.size() - 1]));
                }
            }
            SSTIDtoSSTInfoMap.insert(make_pair(SSTID, sstInfoVec));
        }
    }
    return true;
}

int main(int argc, char **argv) {
    if (argc != 3) {
        exit(0);
    }
    string manifestPathStr(argv[1]);
    string sstLogPathStr(argv[2]);

    ifstream inputLogStream, inputManifestStream;
    inputManifestStream.open(manifestPathStr, ios::in | ios::binary);
    inputLogStream.open(sstLogPathStr, ios::in | ios::binary);
    unordered_map<int, vector<int>> levelIDtoSSTIDMap;
    unordered_map<int, vector<uint64_t>> SSTIDtoSSTInfoMap;
    bool readManifestStatus = readManifestLog(inputManifestStream, levelIDtoSSTIDMap);
    bool readSSTInfoStatus = readSSTInfoLog(inputLogStream, SSTIDtoSSTInfoMap);
    inputManifestStream.close();
    inputLogStream.close();

    for (auto levelIndex : levelIDtoSSTIDMap) {
        cout << "Level ID = " << levelIndex.first << endl;
        uint64_t countResult[3][3] = {0};
        for (auto SSTIndex : levelIndex.second) {
            if (SSTIDtoSSTInfoMap.find(SSTIndex) == SSTIDtoSSTInfoMap.end()) {
                cerr << "Target SST file ID = " << SSTIndex << " not exist" << endl;
                continue;
            }
            for (int dataTypeIndex = 0; dataTypeIndex < 3; dataTypeIndex++) {
                for (int counterIndex = 0; counterIndex < 3; counterIndex++) {
                    countResult[dataTypeIndex][counterIndex] += SSTIDtoSSTInfoMap.at(SSTIndex)[dataTypeIndex * 3 + counterIndex];
                }
            }
        }
        cout << "VALUE: \n\tNumber = " << countResult[VALUE][0]
             << "\n\tkey size (byte) = " << countResult[VALUE][1]
             << "\n\tvalue size (byte) = " << countResult[VALUE][2] << endl;
        cout << "DELTA: \n\tNumber = " << countResult[DELTA][0]
             << "\n\tkey size (byte) = " << countResult[DELTA][1]
             << "\n\tvalue size (byte) = " << countResult[DELTA][2] << endl;
        cout << "POSITION: \n\tNumber = " << countResult[POSITION][0]
             << "\n\tkey size (byte) = " << countResult[POSITION][1]
             << "\n\tvalue size (byte) = " << countResult[POSITION][2] << endl
             << endl;
    }

    return 0;
}