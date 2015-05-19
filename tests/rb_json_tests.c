// rb_json_tests.c 
#undef NDEBUG
#include <assert.h>
#include <jansson.h>
#include <librdkafka.h>

typedef json_error_t rb_json_err_t;

/// you have to json_decref(return) when done
static void *rb_json_assert_unpack(const char *json,size_t flags,const char *fmt,...) __attribute((unused));
static void *rb_json_assert_unpack(const char *json,size_t flags,const char *fmt,...){
	json_error_t error;
	json_t *root = json_loads(json, 0, &error);
	if(root==NULL){
		fprintf(stderr,"[EROR PARSING JSON][%s][%s]\n",error.text,error.source);
		assert(0);
	}

	va_list args;
  	va_start (args, fmt);

	const int unpack_rc = json_vunpack_ex(root,&error,flags,fmt,args);

	if(unpack_rc != 0 /* Failure */){
		fprintf(stderr,"[ERROR UNPACKING][%s][%s]\n",error.text,error.source);
		assert(0);
	}

	va_end(args);

	return root;
}

static void free_json_unpacked(void *mem) __attribute__((unused));
static void free_json_unpacked(void *mem){
	json_decref(mem);
}

static int str_equal(const char *str1,const char *str2) __attribute__((unused));
static int str_equal(const char *str1,const char *str2){
	if((str1!=NULL && str2==NULL) || (str1==NULL && str2!=NULL))
		return 0;
	if(str1==NULL && str2==NULL)
		return 1;
	return 0==strcmp(str1,str2);
}

#ifndef USE_DEPRECATED_STRUCTS

struct checkdata_value{
	const char *key;
	json_type type;
	const char *value;
};

struct checkdata{
	size_t size;
	const struct checkdata_value *checks;
};

static void assertEqual(const int64_t a, const int64_t b,const char *key,const char *src) __attribute__((unused));
static void assertEqual(const int64_t a, const int64_t b,const char *key,const char *src){
	if(a != b) {
		fprintf(stderr,"[%s integer value mismatch] Actual: %ld, Expected: %ld in %s\n",
			key,a,b,src);
		assert(a==b);
	}
}

static void rb_assert_json_value(const struct checkdata_value *chk_value,const json_t *json_value,const char *src)__attribute__((unused));
static void rb_assert_json_value(const struct checkdata_value *chk_value,const json_t *json_value,const char *src){
	//assert(chk_value->type == json_typeof(json_value));
	if(chk_value->value == NULL && json_value == NULL){
		return; // All ok
	}

	if(chk_value->value == NULL && json_value != NULL) {
		fprintf(stderr,"Json key %s with value %s, should not exists in (%s)\n",
			chk_value->key,json_string_value(json_value),src);
		assert(!json_value);		
	}

	if(NULL==json_value) {
		fprintf(stderr,"Json value %s does not exists in %s\n",chk_value->key,src);
		assert(json_value);
	}
	switch(json_typeof(json_value)){
	case JSON_INTEGER:
	{
		const json_int_t json_int_value = json_integer_value(json_value);
		const long chk_int_value = atol(chk_value->value);
		assertEqual(json_int_value,chk_int_value,chk_value->key,src);
	}
	break;
	case JSON_STRING:
	{
		const char *json_str_value = json_string_value(json_value);
		assert(0==strcmp(json_str_value,chk_value->value));
	}
	break;
	default:
		assert(!"You should not be here");
	}
}

static json_t *rb_assert_json_unpack(const char *src) {
	json_error_t error;
	json_t *root = json_loads(str, 0, &error);

	if(root==NULL){
		fprintf(stderr,"[EROR PARSING JSON][%s][%s]\n",error.text,error.source);
		assert(0);
	}

	return root;
}

static void rb_assert_json(const char *str,const struct checkdata *checkdata) __attribute__((unused));
static void rb_assert_json(const char *str,const struct checkdata *checkdata){
	size_t i=0;
	json_t *root = rb_assert_json_unpack(str);

	for(i=0;i<checkdata->size;++i){
		const json_t *json_value = json_object_get(root,checkdata->checks[i].key);
		rb_assert_json_value(&checkdata->checks[i],json_value,str);
	}

	json_decref(root);
}

#endif