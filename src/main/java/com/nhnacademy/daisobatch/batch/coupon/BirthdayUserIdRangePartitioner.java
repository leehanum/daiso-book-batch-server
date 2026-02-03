package com.nhnacademy.daisobatch.batch.coupon;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class BirthdayUserIdRangePartitioner implements Partitioner {

    private final JdbcTemplate jdbcTemplate;

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) { // gridSize 몇개의 파티션(스레드)으로 나눌지

        int currentMonth = LocalDate.now().getMonthValue();

        // 1. 이번달 생일자 중 최소/최대 user_created_id 조회
        String minMaxSql = """
            SELECT MIN(u.user_created_id) as min_id, 
                   MAX(u.user_created_id) as max_id
            FROM Users u
            INNER JOIN Accounts a ON a.user_created_id = u.user_created_id
            WHERE u.birth IS NOT NULL
              AND MONTH(u.birth) = ?
              AND a.current_status_id = 1
              AND NOT EXISTS (
                  SELECT 1 FROM user_coupons uc
                  WHERE uc.user_created_id = u.user_created_id
                    AND uc.coupon_policy_id = 4
              )
        """;

        Map<String, Object> minMax = jdbcTemplate.queryForMap(minMaxSql, currentMonth);
        Long minId = ((Number) minMax.get("min_id")).longValue();
        Long maxId = ((Number) minMax.get("max_id")).longValue();

        log.info("=== 파티셔닝 시작 ===");
        log.info("현재 월: {}월", currentMonth);
        log.info("대상 user_created_id 범위: {} ~ {}", minId, maxId);

        // 2. user_created_id 범위를 gridSize만큼 분할
        long rangeSize = (maxId - minId + 1 ) / gridSize;

        Map<String, ExecutionContext> partitions = new HashMap<>();

        for (int i = 0; i < gridSize; i++) {
            long startId = minId + (i * rangeSize);
            long endId = (i == gridSize - 1)
                    ? maxId  // 마지막 파티션은 나머지 전부
                    : startId + rangeSize - 1;

            ExecutionContext context = new ExecutionContext(); // ExecutionContext란? Spring Batch가 각 파티션 Step마다 하나씩 복사해서 전달
            // 각 파티션이 서로 다른 범위를 처리하도록 필요한 정보를 ExecutionContext에 담아 넘겨주는 것
            context.putLong("minUserCreatedId", startId);
            context.putLong("maxUserCreatedId", endId);
            context.putInt("currentMonth", currentMonth);

            partitions.put("partition-" + i, context);

            log.info("파티션 {} 생성: user_created_id {} ~ {} (예상 {}건)",
                    i, startId, endId, rangeSize);
        }

        log.info("총 {}개 파티션 생성 완료", gridSize);

        return partitions;
    }
}
