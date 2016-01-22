/**
 * @Email:1768880751@qq.com
 * @Author:zhl
 * @Date:2016年1月22日下午5:21:03
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.elastic.client;

import java.net.UnknownHostException;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.FilteredQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;


public class TestQuery {

	// @Test
	public void testFilter() throws UnknownHostException {

		FilterBuilder filter = FilterBuilders.queryFilter(QueryBuilders.queryStringQuery("18917967166").field("mdn"));

		RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("day").from("20151101").to("20151223");

		FilteredQueryBuilder query1 = QueryBuilders.filteredQuery(rangeQuery, filter);

		SearchResponse response = ElasticFactory.getClient().prepareSearch("tags").setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(query1).setFrom(0).setSize(60).setExplain(true).execute().actionGet();
		System.out.println(response);
		ElasticFactory.close();
	}

	public void TestMatch() throws UnknownHostException {

	}

	//@Test
	public void testBoo() throws UnknownHostException {
		FilterBuilder filter = FilterBuilders.queryFilter(QueryBuilders.queryStringQuery("18917967166").field("mdn"));

		RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("day").from("20151101").to("20151223");

		FilteredQueryBuilder query1 = QueryBuilders.filteredQuery(rangeQuery, filter);

		BoolQueryBuilder boolQuery = QueryBuilders.boolQuery().must(query1);

		FilterBuilder filter2 = FilterBuilders.queryFilter(QueryBuilders.queryStringQuery("18050077670").field("mdn"));

		
		FilteredQueryBuilder queryBig = QueryBuilders.filtered(boolQuery,filter2);
		
		
		SearchResponse response = ElasticFactory.getClient().prepareSearch("tags").setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
				.setQuery(queryBig)
				.setQuery(boolQuery)
				.setFrom(0)
				.setSize(60)
				.setExplain(true)
				.execute()
				.actionGet();
		System.out.println(response);
		ElasticFactory.close();

	}

	//@Test
	public void testMatch(){
		MatchQueryBuilder matchQuery1=QueryBuilders
				.matchQuery("mdn", "18036404545").operator(Operator.OR);
		MatchQueryBuilder matchQuery2=QueryBuilders
				.matchQuery("mdn", "18036404545").operator(Operator.OR);
		
		BoolQueryBuilder boolQuery = QueryBuilders.boolQuery().must(matchQuery1).must(matchQuery2);
		
		
		SearchResponse response = ElasticFactory.getClient().prepareSearch("tags")
				.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
				.setQuery(boolQuery)
				//.setQuery(boolQuery)
				.setFrom(0)
				.setSize(60)
				.setExplain(true)
				.execute()
				.actionGet();
		System.out.println(response);
		ElasticFactory.close();
	}
}
