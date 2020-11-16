package com.sys.es;


import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sys.es.util.HtmlTool;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.junit.After;
import org.junit.Test;
import org.jvnet.hk2.annotations.Service;

import java.io.File;
import java.io.IOException;
import java.util.*;

@Service
@Slf4j
public class IndexService {

    //存放html文件的目录
    public static String DATA_DIR = "C:\\Users\\180619\\Desktop\\data";

    private static ObjectMapper mapper = new ObjectMapper();
    public static RestHighLevelClient client;

    static {
        client =
                new RestHighLevelClient(RestClient.builder(new HttpHost("192.168.232.30", 9200, "http"),
                        new HttpHost("192.168.232.31", 9200, "http"),
                        new HttpHost("192.168.232.32", 9200, "http")));

    }

    @After
    public void test() {
        if (client != null) {
            try {
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 根据 index, id 查询数据
     */
    @Test
    public void test1() {
        try {
            RequestOptions aDefault = RequestOptions.DEFAULT;
//			aDefault.toBuilder().addHeader("ontent-Type", "application/json");
            GetResponse getResponse = client.get(new GetRequest("yang_zzu", "1"), aDefault);
            System.out.println(getResponse);
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 根据 字段进行查询
     */
    @Test
    public void test2() throws IOException {
        BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();
        /**
         * 这里可以根据字段进行搜索，must表示符合条件的，相反的mustnot表示不符合条件的
         */
        // match_phrase 完全匹配，不进行拆词
//		boolBuilder.must(QueryBuilders.matchPhraseQuery("about", "hello"));
        //match 进行拆词，然后将拆开的词进行 or 匹配，匹配到一个就可以
//		boolBuilder.must(QueryBuilders.matchQuery("about", "hello love"));
        // 多个字段进行搜索， 第一个字段：搜索的内容，会进行拆词， 后面一次写入需要 匹配的字段
        boolBuilder.must(QueryBuilders.multiMatchQuery("xiaohong hello", "first_name", "about"));
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(boolBuilder);
        sourceBuilder.from(0);
        sourceBuilder.size(100); // 获取记录数，默认10
        // 第一个是获取字段，第二个是过滤的字段，默认获取全部
//		sourceBuilder.fetchSource(new String[] { "last_name", "age" }, new String[] {});
        sourceBuilder.fetchSource(new String[]{}, new String[]{});

        //索引，那个库
        SearchRequest searchRequest = new SearchRequest("yang_zzu");
        //类型type， 那个表
        searchRequest.types("employee");
        searchRequest.source(sourceBuilder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        System.out.println(response.toString());

        SearchHits hits = response.getHits();
        SearchHit[] searchHits = hits.getHits();
        //searchHits[0].getSourceAsMap().get("age")
        for (SearchHit hit : searchHits) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
//			System.out.println(sourceAsMap.toString());
            String s = JSONObject.toJSONString(sourceAsMap);
            System.out.println(s);

        }

    }


    /**
     * 检查索引
     *
     * @param index
     * @return
     * @throws IOException
     */
    public static boolean checkIndexExist(String index) throws IOException {
        GetIndexRequest request = new GetIndexRequest(index);
        request.indices();
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        return exists;
    }

    /**
     * 获取低水平客户端
     *
     * @return
     */
    public static RestClient getLowLevelClient() {
        return client.getLowLevelClient();
    }

    /**
     * 获取记录信息
     *
     * @param index
     * @param type
     * @param id
     * @throws IOException
     */
    public static Map<String, Object> getJson(String index, String type, String id) throws IOException {
        GetRequest getRequest = new GetRequest(index, type, id);
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        return getResponse.getSource();
    }

    /**
     * 插入记录信息
     *
     * @param index
     * @param type
     * @param object
     */
    public static void insert(String index, String type, String id, JSONObject object) throws IOException {
        IndexRequest indexRequest = new IndexRequest(index, type, id);

        indexRequest.source(mapper.writeValueAsString(object), XContentType.JSON);
        client.index(indexRequest, RequestOptions.DEFAULT);
    }

    /**
     * 更新记录信息
     *
     * @param index
     * @param type
     * @param id
     * @param object
     * @throws IOException
     */
    public static void update(String index, String type, String id, JSONObject object) throws IOException {
        UpdateRequest request = new UpdateRequest(index, type, id);
        request.doc(object, XContentType.JSON);
        client.update(request, RequestOptions.DEFAULT);
    }

    /**
     * 删除记录
     *
     * @param index
     * @param type
     * @param id
     * @throws IOException
     */
    public static void delete(String index, String type, String id) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest(index, type, id);
        client.delete(deleteRequest, RequestOptions.DEFAULT);
    }


    /**
     *  查询
     *
     * @param index 索引库
     * @param type 表
     * @param name 查询的数据
     * @throws IOException
     */
    public static java.util.List<String> search(String index, String type, String name) throws IOException {
        BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();
        // match_phrase 完全匹配，不进行拆词
//        boolBuilder.must(QueryBuilders.matchPhraseQuery("content", name));
        //match 进行拆词，然后将拆开的词进行 or 匹配，匹配到一个就可以
//		boolBuilder.must(QueryBuilders.matchQuery("content", name));
        // 多个字段进行搜索， 第一个字段：搜索的内容，会进行拆词， 后面一次写入需要 匹配的字段
		boolBuilder.must(QueryBuilders.multiMatchQuery(name,"content","title"));
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(boolBuilder);
		// 获取记录数，默认10
        sourceBuilder.from(0);
        sourceBuilder.size(100);
		// 第一个是获取字段，第二个是过滤的字段，默认获取全部
		sourceBuilder.fetchSource(new String[]{"id", "title", "content", "url"}, new String[]{});
		HighlightBuilder highlightBuilder = addHeightLight();
		//配置高亮
		sourceBuilder.highlighter(highlightBuilder);
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.types(type);
        searchRequest.source(sourceBuilder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        List<String> list = new LinkedList<>();
        SearchHits hits = response.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            Object url = hit.getSourceAsMap().get("url");
            Object title = hit.getSourceAsMap().get("title");
            Object content = hit.getSourceAsMap().get("content");

//            String s = JSONObject.toJSONString(hit);
//            System.out.println(s);

            //获取高亮字段
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            HighlightField titleField = highlightFields.get("title");//高亮后的标题
            HighlightField contentField = highlightFields.get("content");//高亮后的内容
            String id = hit.getId();
//			list.add(hit.getSourceAsString());
            list.add(id);
            list.add(url.toString());
            list.add(title.toString());
            if (titleField != null) {
                Text[] fragments = titleField.getFragments();
                list.add(fragments[0].toString());
            }
            if (contentField != null) {
                Text[] fragments = contentField.getFragments();
                list.add(fragments[0].toString());
            }
            list.add("********************************************");
        }
        return list;
    }

    /**
     * 增加高亮
     */
    private static HighlightBuilder addHeightLight() {
        HighlightBuilder highlightBuilder = new HighlightBuilder(); //生成高亮查询器
		highlightBuilder.field("title");      //高亮查询字段
        highlightBuilder.field("content");    //高亮查询字段
        highlightBuilder.requireFieldMatch(false);     //如果要多个字段高亮,这项要为false
        highlightBuilder.preTags("<span style=\"color:red\">");   //高亮设置
        highlightBuilder.postTags("</span>");

        //下面这两项,如果你要高亮如文字内容等有很多字的字段,必须配置,不然会导致高亮不全,文章内容缺失等
        highlightBuilder.fragmentSize(800000); //最大高亮分片数
        highlightBuilder.numOfFragments(0); //从第一个分片获取高亮片段
        return highlightBuilder;
    }


    /**
     * 创建索引库
     * index名必须全小写，否则报错
     */
    @Test
    public void createIndex() throws Exception {
        // 判断，索引库是否存在，如果存在，则进行删除
        boolean test2 = client.indices().exists(new GetIndexRequest("test1"), RequestOptions.DEFAULT);
        if (test2) {
            client.indices().delete(new DeleteIndexRequest("test1"), RequestOptions.DEFAULT);
        }

        try {
            // 分词器设置
            XContentBuilder builder = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("htmlbean")
                    .startObject("properties")
                    .startObject("title")
                    .field("type", "text")
                    .field("store", "true")
                    .field("analyzer", "ik_max_word")
                    .field("search_analyzer", "ik_max_word")
                    .endObject()
                    .startObject("content")
                    .field("type", "text")
                    .field("store", "true")
                    .field("analyzer", "ik_max_word")
                    .field("search_analyzer", "ik_max_word")
                    .endObject()
//				.startObject("url")
//				.field("type", "test")
//				.field("store", "true")
//				.field("analyzer", "ik_max_word")
//				.field("search_analyzer", "ik_max_word")
//				.endObject()
                    .endObject().endObject().endObject();

            CreateIndexRequest createIndexRequest = new CreateIndexRequest("test1");
            System.out.println(builder.toString());
            createIndexRequest.mapping("htmlbean", builder);
            //设置， shards 切片为 9 ，设置副本为 2
            Settings.Builder put = Settings.builder().put("index.number_of_shards", 9)
                    .put("index.number_of_replicas", 2);
            createIndexRequest.settings(put);

            CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            boolean acknowledged = createIndexResponse.isAcknowledged();
            if (acknowledged) {
                System.out.println("success!!!!!!!!!!!!!!!!!!!!!!!!!!");
            } else {
                System.err.println("err.............................");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * 遍历数据文件目录d:/data ，递归方法
     *
     * @param file
     */
    public void readHtml(File file) {
        //如果 file 是一个文件夹，进行递归
        if (file.isDirectory()) {
            File[] fs = file.listFiles();
            for (int i = 0; i < fs.length; i++) {
                File f = fs[i];
                readHtml(f);
            }
        } else {
            HtmlBean bean;
            try {
                bean = HtmlTool.parserHtml(file.getPath());
                if (bean != null) {
                    Map<String, String> dataMap = new HashMap<String, String>();
                    dataMap.put("title", bean.getTitle());
                    dataMap.put("content", bean.getContent());
                    dataMap.put("url", bean.getUrl());
                    //如果索引不存在，则进行创建
                    if (!checkIndexExist("test1")) {
                        createIndex();
                    }
                    //添加数据
					String uuid = UUID.randomUUID().toString().replaceAll("-", "");
					insert("test1", "htmlbean", uuid, JSONObject.parseObject(JSONObject.toJSONString(dataMap)));
                    System.out.println(uuid);
                }
            } catch (Throwable e) {
                e.printStackTrace();
            }

        }
    }


	/**
     * 插入数据
	 * 把源数据html文件添加到索引库中（构建索引文件）
	 */
	@Test
	public void addHtmlToES(){
		readHtml(new File(DATA_DIR));
	}


    /**
     * 测试，搜索
     * 形成类似于百度的 搜索页面，
     * 标题，内容，关键字进行高亮显示
     */
	@Test
    public void testSearch() {
        try {
            List<String> search = search("test1", "htmlbean", "文化");
            search.forEach(s -> {
                System.out.println(s.toString());
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }




}
