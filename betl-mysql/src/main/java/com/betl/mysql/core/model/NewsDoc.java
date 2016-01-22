
package com.betl.mysql.core.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

public class NewsDoc implements Writable, DBWritable {

	private String url;
	private String title;
	private String content;

	public void write(PreparedStatement statement) throws SQLException {
		statement.setString(1, this.url);
		statement.setString(2, this.title);
		statement.setString(3, this.content);
	}

	public void readFields(ResultSet resultSet) throws SQLException {
		this.url = resultSet.getString("url");
		this.title = resultSet.getString("title");
		this.content = resultSet.getString("content");
	}

	public void write(DataOutput out) throws IOException {
		Text.writeString(out, this.url);
		Text.writeString(out, this.title);
		Text.writeString(out, this.content);
	}

	public void readFields(DataInput in) throws IOException {
		this.url = Text.readString(in);
		this.title = Text.readString(in);
		this.content = Text.readString(in);
	}

	@Override
	public String toString() {
		return url + "\t" + title + "\t" + replaceBlank(content);
	}

	public String replaceBlank(String str) {
		String dest = "";
		if (str != null) {
			Pattern p = Pattern.compile("\\s*|\t|\r|\n");
			Matcher m = p.matcher(str);
			dest = m.replaceAll("");
		}
		return dest;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

}
