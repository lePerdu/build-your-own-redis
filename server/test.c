// NOLINTBEGIN(bugprone-suspicious-include)
#include "test_hashmap.c"
#include "test_parser.c"
// NOLINTEND(bugprone-suspicious-include)

int main(void) {
  test_parser();
  test_hashmap();

  return 0;
}
