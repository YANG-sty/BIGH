package com.sys.hadoop.mr.pagerank;



import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Arrays;

/**
 * 保存数据的节点
 *
 * Create by yang_zzu on 2020/8/28 on 16:32
 */
public class Node {
    private double pageRank = 1.0;
    private String[] adjacentNodeNames;

    // 切分数据的标识符
    public static final char fieldSeparator = '\t';


    /**
     * 是否含有数据
     */
    public boolean containsAdjacentNodes() {
        return adjacentNodeNames != null && adjacentNodeNames.length > 0;
    }

    public static Node fromMR(String value) throws IOException {
        String[] parts = StringUtils.splitPreserveAllTokens(value, fieldSeparator);
        if (parts.length < 1) {
            throw new IOException("Expected 1 or more parts but received " + parts.length);
        }
        Node node = new Node().setPageRank(Double.valueOf(parts[0]));
        if (parts.length > 1) {
            node.setAdjacentNodeNames(Arrays.copyOfRange(parts, 1, parts.length));
        }
        return node;
    }

    public static Node fromMR(String v1, String v2) throws IOException {
        return fromMR(v1 + fieldSeparator + v2);
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(pageRank);
        if (getAdjacentNodeNames() != null) {
            sb.append(fieldSeparator).append(StringUtils.join(getAdjacentNodeNames(), fieldSeparator));
        }
        return sb.toString();
    }

    public double getPageRank() {
        return pageRank;
    }

    public Node setPageRank(double pageRank) {
        this.pageRank = pageRank;
        return this;
    }

    public String[] getAdjacentNodeNames() {
        return adjacentNodeNames;
    }

    public Node setAdjacentNodeNames(String[] adjacentNodeNames) {
        this.adjacentNodeNames = adjacentNodeNames;
        return this;
    }
}
