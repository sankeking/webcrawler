package idv.ccc.crawler;

import java.io.*;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

public class crawler implements Runnable {
	private static String url;
	private Document document;

	public crawler(String url) {
		crawler.url = url;
	}

	@Override
	public void run() {
		try {
			// 爬所需網頁的連接
			document = Jsoup.connect(url).timeout(3000).get();
			// 連接資料庫
			String driver = "com.mysql.cj.jdbc.Driver";
			String url = "jdbc:mysql://25.8.87.250:3306/oprasys";
			String user = "opsys";
			String password = "opsyspass";

			Class.forName(driver);
			Connection conn = DriverManager.getConnection(url, user, password);
			//下SQL語法去抓出目前資料表的資料
			Statement statement = conn.createStatement();
			String sql = "SELECT * FROM movie;";
			ResultSet rs = statement.executeQuery(sql);
			// 只抓出資料表的電影名稱
			String n = null;
			ArrayList<String> na = new ArrayList<String>(1000);

			while (rs.next()) {
				n = rs.getString("name");
				na.add(n);
			}
			// 將資料insert進去資料表
			String SQL_INSERT = "INSERT INTO movie (Name, Date, score) " + "VALUES (?, ?, ?);";
			PreparedStatement ps = conn.prepareStatement(SQL_INSERT);
			// 用迴圈抓去網頁的資料
			for (int i = 1; i > 0; i++) {
				// 取得資料
				Elements links = document
						.select("#main > div > span > div > div > div.lister > table > tbody > tr:nth-child(" + i
								+ ") > td.titleColumn > a");
				Elements rates = document
						.select("#main > div > span > div > div > div.lister > table > tbody > tr:nth-child(" + i
								+ ") > td.ratingColumn.imdbRating > strong");
				Elements years = document
						.select("#main > div > span > div > div > div.lister > table > tbody > tr:nth-child(" + i
								+ ") > td.titleColumn > span");
				// 將年份資料去做整理
				String str = years.text().replace("(", "").replace(")", "");
				// 判斷所需資料
				try {
					// 判斷名稱不為空白
					if (links.text() == "") {
						break;
					} else {
						// 判斷資料不存在資料表以及分數不為空白
						if (na.contains(links.text()) || rates.text() == "") {
							continue;

						} else {
							// insert部分 先將資料整理好
							ps.setString(1, links.text());
							ps.setString(2, str);
							ps.setString(3, rates.text());
							ps.addBatch();
							// System.out.print("text" + i + " : " + links.text());
							// System.out.print("rate" + i + " : " + str);
							// System.out.println("rate" + i + " : " + rates.text());
						}
					}
				} catch (Exception e) {
					System.out.println("IF有錯");
					continue;
				}
			}
			// 執行insert動作
			ps.executeBatch();
			conn.close();
			System.out.println("爬蟲結束");
			
		} catch (IOException e) {
			System.out.println("爬蟲呼叫失敗");
		} catch (ClassNotFoundException e) {
			System.out.println("找不到驅動程式類別");
		} catch (SQLException e) {
			System.out.println("SQL失敗");
		}
	}

	public static void main(String[] args) throws InterruptedException {

		System.out.println("爬蟲開始");
		// 將爬蟲網頁連結放入urls
		ArrayList<String> urls = new ArrayList<String>();
		urls.add("https://www.imdb.com/chart/top/?ref_=nv_mv_250");
		urls.add("https://www.imdb.com/chart/moviemeter?pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=4da9d9a5-d299-43f2-9c53-f0efa18182cd&pf_rd_r=K0G2ZDMV9EGFAPEK4D3Z&pf_rd_s=right-4&pf_rd_t=15506&pf_rd_i=top&ref_=chttp_ql_2");
		// 設多執行緒
		ExecutorService executor = Executors.newCachedThreadPool();
		try {
			for (int i = 0; i < urls.size(); i++) {
				// 利用多執行緒執行爬蟲
				executor.execute(new crawler(urls.get(i)));
				Thread.sleep(1000);
			}
		} catch (Exception e) {
			System.out.println("執行緒出錯");
		}
		System.out.println("執行緒數量："+ urls.size());

		executor.shutdown();
	}
}
