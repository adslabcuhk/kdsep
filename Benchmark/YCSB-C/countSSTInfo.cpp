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

int main(int argc, char **argv) {
    if (argc != 2) {
        exit(0);
    }
    string fileNameStr(argv[1]);
    ifstream inputLogStream;
    inputLogStream.open(fileNameStr, ios::in | ios::binary);
    string currentLine;
    uint64_t countResult[3][3] = {0};
    while (getline(inputLogStream, currentLine)) {
        int dataType = 0, keySize = 0, dataSize = 0;
        if (currentLine.find("type") != string::npos) {
            lineProcess(currentLine, dataType, keySize, dataSize);
            switch (dataType) {
                case 1:
                    //   cout << "VALUE"
                    //        << "\t" << keySize << "\t" << dataSize << endl;
                    countResult[VALUE][0] += keySize;
                    countResult[VALUE][1] += dataSize;
                    countResult[VALUE][2]++;
                    break;
                case 2:
                    //   cout << "DELTA"
                    //        << "\t" << keySize << "\t" << dataSize << endl;
                    countResult[DELTA][0] += keySize;
                    countResult[DELTA][1] += dataSize;
                    countResult[DELTA][2]++;
                    break;
                case 17:
                    //   cout << "POSITION"
                    //        << "\t" << keySize << "\t" << dataSize << endl;
                    countResult[POSITION][0] += keySize;
                    countResult[POSITION][1] += dataSize;
                    countResult[POSITION][2]++;
                    break;
                default:
                    cerr << "Process error, undefined data type" << endl;
                    break;
            }
        }
    }
    cout << "VALUE: \n\tNumber = " << countResult[VALUE][2]
         << "\n\tkey size (byte) = " << countResult[VALUE][0]
         << "\n\tvalue size (byte) = " << countResult[VALUE][1] << endl;
    cout << "DELTA: \n\tNumber = " << countResult[DELTA][2]
         << "\n\tkey size (byte) = " << countResult[DELTA][0]
         << "\n\tvalue size (byte) = " << countResult[DELTA][1] << endl;
    cout << "POSITION: \n\tNumber = " << countResult[POSITION][2]
         << "\n\tkey size (byte) = " << countResult[POSITION][0]
         << "\n\tvalue size (byte) = " << countResult[POSITION][1] << endl;
    inputLogStream.close();
    return 0;
}