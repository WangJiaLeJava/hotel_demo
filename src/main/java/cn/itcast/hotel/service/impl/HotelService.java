package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.apache.lucene.queryparser.xml.builders.BooleanQueryBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.RequestBody;

import java.io.IOException;
import java.util.*;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {
    @Autowired
    RestHighLevelClient client;
    @Override
    public PageResult search(RequestParams params) {
        //准备request
        try {
            SearchRequest request=new SearchRequest("hotel");
            //准备DSL
            //发送请求
            termSearch(params, request);

            //分页
            int page = params.getPage();
            int size = params.getSize();
            request.source().from((page-1)*size).size(size);
            String location = params.getLocation();
            if (location !=null&& !location.equals("")){
                request.source().sort(
                        SortBuilders.geoDistanceSort("location",new GeoPoint(location))
                                .order(SortOrder.ASC)
                                .unit(DistanceUnit.KILOMETERS)
                );
            }


            SearchResponse search = client.search(request, RequestOptions.DEFAULT);
            //解析响应
            return  handleSearch(search);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, List<String>> filters(RequestParams params) {
        try {
            SearchRequest request = new SearchRequest("hotel");
            termSearch(params,request);
            request.source().size(0);
            //把三个请求方式全部一次性写完得到完整的数据
            buildAggration(request);
            //拿到数据
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);

            Map<String,List<String>>result=new HashMap<>();
            Aggregations aggregations = response.getAggregations();
            //调用方法 灵活的传参拿到各个集合
            List<String> brandList = getAggBrand(aggregations,"brandAgg");
            result.put("brand",brandList);

            List<String> cityList = getAggBrand(aggregations,"cityAgg");
            result.put("city",cityList);

            List<String> starList = getAggBrand(aggregations,"starAgg");
            result.put("starName",starList);


            return result;
        } catch (IOException e) {
            throw  new RuntimeException();
        }


    }

    private List<String> getAggBrand(Aggregations aggregations ,String aggName) {

        Terms brandAgg = aggregations.get(aggName);
        List<String>brandList=new ArrayList<>();
        List<? extends Terms.Bucket> buckets = brandAgg.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            String key= bucket.getKeyAsString();
            brandList.add(key);
        }
        return brandList;
    }

    private void buildAggration(SearchRequest request) {
        request.source().aggregation(AggregationBuilders
                .terms("brandAgg")
                .field("brand")
                .size(200));
        request.source().aggregation(AggregationBuilders
                .terms("cityAgg")
                .field("city")
                .size(200));

        request.source().aggregation(AggregationBuilders
                .terms("starAgg")
                .field("starName")
                .size(200));
    }

    private void termSearch(RequestParams params, SearchRequest request) {
        BoolQueryBuilder boolQuery=QueryBuilders.boolQuery();
        //准备dsl
        String key = params.getKey();
        if (key==null||"".equals(key)) {
            boolQuery.must(QueryBuilders.matchAllQuery());
        }else {
            boolQuery.must(QueryBuilders.matchQuery("all", key));
        }
        if (params.getCity()!=null && !params.getCity().equals("")){
            boolQuery.filter(QueryBuilders.termQuery("city", params.getCity()));
        }
        if (params.getBrand()!=null && params.getBrand().equals("")){
            boolQuery.filter(QueryBuilders.termQuery("brand", params.getBrand()));
        }
        if (params.getStarName()!=null && params.getStarName().equals("")){
            boolQuery.filter(QueryBuilders.termQuery("starName", params.getStarName()));
        }
        if (params.getMaxPrice()!=null&& params.getMinPrice()!=null){
            boolQuery.filter(QueryBuilders.rangeQuery("price")
                    .gte(params.getMinPrice()).lte(params.getMaxPrice()));
        }

        FunctionScoreQueryBuilder functionScoreQueryBuilder=
                QueryBuilders.functionScoreQuery(
                        boolQuery,
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                        QueryBuilders.termQuery("isAD",true),
                                        ScoreFunctionBuilders.weightFactorFunction(10)
                                )
                        });
        request.source().query(functionScoreQueryBuilder);
    }


    private PageResult handleSearch(SearchResponse search) {

        SearchHits searchHits = search.getHits();
        //获取总数
        long total = searchHits.getTotalHits().value;
        List<HotelDoc>hotels=new ArrayList<>();
        //内容
        SearchHit[] hits= searchHits.getHits();
        for (SearchHit hit : hits) {
            String json = hit.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            Object[] sortValues = hit.getSortValues();
            if (sortValues.length>0){
                Object sortValue = sortValues[0];
                hotelDoc.setDistance(sortValue);
            }
            hotels.add(hotelDoc);
        }
        return new PageResult(total,hotels);
    }
}
