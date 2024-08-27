void test_parser(void);
void test_writer(void);
void test_hashmap(void);
void test_avl(void);
void test_heap(void);

int main(void) {
  test_parser();
  test_writer();
  test_hashmap();
  test_avl();
  test_heap();

  return 0;
}
