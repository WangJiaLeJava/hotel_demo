package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import cn.itcast.hotel.service.impl.HotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.api.R;
import com.fasterxml.jackson.databind.util.ArrayBuilders;
import org.apache.http.HttpHost;
import org.apache.lucene.queryparser.xml.builders.BooleanQueryBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
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
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.similarity.ScriptedSimilarity;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.flyway.FlywayDataSource;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.CollectionUtils;


import javax.naming.directory.SearchResult;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static cn.itcast.hotel.constant.HotelConstants.MAPPING_TEMPLATE;
@SpringBootTest
public class HotelIndexTest {
    @Autowired
    private RestHighLevelClient client;
    @Autowired
    private IHotelService hotelService;
    @Test
    void testAddDocument() throws IOException {
        Hotel hotel = hotelService.getById(61083L);
        HotelDoc hotelDoc=new HotelDoc(hotel);
        IndexRequest request=new IndexRequest("hotel").id(hotel.getId().toString());
        request.source(JSON.toJSONString(hotelDoc),XContentType.JSON);
        client.index(request,RequestOptions.DEFAULT);
    }
     @Test
     void testGetDocumentById() throws IOException {
         GetRequest request=new GetRequest("hotel","61083");
         GetResponse response=client.get(request,RequestOptions.DEFAULT);
         String json = response.getSourceAsString();
         HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
         System.out.println(hotelDoc);
     }

    @Test
    void testUpdateDocument() throws IOException {
        UpdateRequest request = new UpdateRequest("hotel", "61083");
        request.doc(
                "price", "952",
                "starName", "四钻"
        );
        client.update(request, RequestOptions.DEFAULT);
     }
     @Test
     void testDeleteDocument() throws IOException {
         DeleteRequest request=new DeleteRequest("hotel", "61083");
         client.delete(request,RequestOptions.DEFAULT);
     }
     //批量增加
     @Test
     void testBulkRequest() throws IOException {
        //批量查询酒店数据
         List<Hotel> hotels = hotelService.list();
         BulkRequest request=new BulkRequest();
         for (Hotel hotel:hotels) {
             HotelDoc hotelDoc=new HotelDoc(hotel);
             request.add(new IndexRequest("hotel")
                     .id(hotelDoc.getId().toString())
                     .source(JSON.toJSONString(hotelDoc),XContentType.JSON));
         }
          client.bulk(request, RequestOptions.DEFAULT);

     }
    @Test
    void testInit() {
        System.out.println(client);
    }

    @BeforeEach
    void setUp() {
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.79.129:9200")
        ));
    }
/*

    //创建索引库
    @Test
    void createHotelIndex() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest("hotel");
        request.source(MAPPING_TEMPLATE, XContentType.JSON);
        client.indices().create(request, RequestOptions.DEFAULT);
    }

    //删除索引库
    @Test
    void testDeleteHotelIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("hotel");
        client.indices().delete(request, RequestOptions.DEFAULT);
    }

    //判断索引库是否纯在
    @Test
    void testExistsHotelIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("hotel");
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }
*/
    //实战文档操作！！！！！  查询全部
    @Test
    void testSearchMatchAll() throws IOException {
        //准备request
        SearchRequest request=new SearchRequest("hotel");
        //准备sql
        request.source().query(QueryBuilders.matchAllQuery());
        //发送请求
        SearchResponse search = client.search(request, RequestOptions.DEFAULT);
        //解析响应
        handleSearch(search);
    }
    //全文检索！！！
    //match 和multiMatchQuery
    @Test
    void testSearchMatch() throws IOException {
         SearchRequest request=new SearchRequest("hotel");
//         request.source().query(QueryBuilders.matchQuery("all", "如家"));
         request.source().query(QueryBuilders.multiMatchQuery("如家","name","business"));
        //发送请求
        SearchResponse search = client.search(request, RequestOptions.DEFAULT);
        //解析响应
        handleSearch(search);
    }
    //精确查询 term和range
    @Test
    void testTerm() throws IOException {
        SearchRequest request=new SearchRequest("hotel");
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.termQuery("city","上海"));
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte(100).lte(150));
        request.source().query(boolQueryBuilder);
        SearchResponse search = client.search(request,RequestOptions.DEFAULT);
        handleSearch(search);
    }
//handle共性方法
    private void handleSearch(SearchResponse search) {

        SearchHits searchHits = search.getHits();
        //获取总数
        long total = searchHits.getTotalHits().value;
        System.out.println("共搜到"+total);
        //内容
        SearchHit[] hits= searchHits.getHits();
        for (SearchHit hit : hits) {
            String json = hit.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            if (!CollectionUtils.isEmpty(highlightFields)) {
                HighlightField highlightField = highlightFields.get("name");
                if (highlightField!=null) {
                    String hightName = highlightField.getFragments()[0].string();
                    hotelDoc.setName(hightName);
                }
            }
            System.out.println("hotelDoc ="+hotelDoc);
        }
    }
    //分页
    @Test
    void testPage() throws IOException {
        int page=2;
        int size=5;
        SearchRequest request=new SearchRequest("hotel");
        request.source().sort("price", SortOrder.ASC);
        request.source().from((page-1)*size).size(5);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        handleSearch(response);
    }

    @Test
    void testHighlight() throws IOException {

        SearchRequest request=new SearchRequest("hotel");
        request.source().query(QueryBuilders.matchQuery("all", "如家"));
        request.source().highlighter(new HighlightBuilder().field("name").requireFieldMatch(false));

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //直接在公共方法里改了这里
        handleSearch(response);
    }
    //restAPI实现聚合
    @Test
    void testAggregations() throws IOException {
        SearchRequest request=new SearchRequest("hotel");
        request.source().size(0);
        request.source().aggregation(AggregationBuilders
                .terms("brandAgg")
                .field("brand")
                .size(10)
        );
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        Aggregations aggregations = response.getAggregations();
        Terms brandTerms = aggregations.get("brandAgg");
        List<? extends Terms.Bucket> buckets = brandTerms.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            String key = bucket.getKeyAsString();
            System.out.println(key);
        }

    }
    @AfterEach
    void tearDown() throws IOException {
        this.client.close();
    }

}
