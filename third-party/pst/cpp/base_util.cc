#include "base_util.h"

vector<unsigned char> intToBytes(int num) {
  // translate integer to byte array
  vector<unsigned char> array(4);
  for(int i=0;i<4;++i)
    array[3-i] = static_cast<unsigned char>(num >> (i * 8));
  return array;
}

int bytesToInt(vector<unsigned char> &vec) {
  // translate byte array to integer
  int result = 0;
  for(int i=0;i<4;++i) {
    int x = vec[3-i];
    result |= x << (i * 8);
  }
  return result;
}

int bytesToInt(FILE *fp) {
  int result = 0;
  unsigned char s[5];
  fread(s, sizeof(unsigned char), 4, fp);
  for(int i=0;i<4;++i) {
    int x = s[3-i];
    result |= x << (i * 8);
  }
  return result;
}

vector<unsigned char> longlongToBytes(long long num) {
  vector<unsigned char> array(8);
  for(int i=0;i<8;++i)
    array[7-i] = static_cast<unsigned char>(num >> (i * 8));
  return array;
}

long long bytesToLonglong(vector<unsigned char> &vec) {
  long long result = 0;
  for(int i=0;i<8;++i) {
    long long x = vec[7-i];
    result |= x << (i * 8);
  }
  return result;
}

long long bytesToLonglong(FILE *fp) {
  long long result = 0;
  unsigned char s[10];
  fread(s, sizeof(unsigned char), 8, fp);
  for(int i=0;i<8;++i) {
    long long x = s[7-i];
    result |= x << (i * 8);
  }
  return result;
}

long long timeToLonglong(char *time_string) {
  // this is for parsing the string from Weiru
  //
  // example:
  //  2001-11-28T13:32:25Z
  size_t len = strlen(time_string);
  long long result = 0;
  for(size_t i=0;i<len;++i)
    if(time_string[i] >= '0' && time_string[i] <= '9')
      result = result * 10 + time_string[i] - '0';
  return result;
}
