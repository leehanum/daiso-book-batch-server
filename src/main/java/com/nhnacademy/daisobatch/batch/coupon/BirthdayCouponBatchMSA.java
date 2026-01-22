package com.nhnacademy.daisobatch.batch.coupon;

import com.nhnacademy.daisobatch.client.BirthdayCouponBulkEvent;
import com.nhnacademy.daisobatch.client.UserServiceClient;
import com.nhnacademy.daisobatch.dto.BirthdayUserDto;
import com.nhnacademy.daisobatch.exception.RabbitPublishFailedException;
import com.nhnacademy.daisobatch.exception.UserServicePagingFailedException;
import com.nhnacademy.daisobatch.listener.JobFailureNotificationListener;
import feign.FeignException;
import feign.RetryableException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalDate;
import java.util.List;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class BirthdayCouponBatchMSA {

    private final UserServiceClient userServiceClient;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final RabbitTemplate rabbitTemplate;

    private final JobFailureNotificationListener jobFailureNotificationListener;

    @Value("${rabbitmq.birthday.exchange}")
    private String birthdayExchange;

    @Value("${rabbitmq.birthday.routing-key}")
    private String birthdayRoutingKey;

    // ===== 1) Job (MSA) =====
    @Bean(name = "birthdayCouponJobMSA")
    public Job birthdayCouponJobMSA(@Qualifier("birthdayCouponStepMSA") Step step) {
        return new JobBuilder("birthdayCouponJobMSA", jobRepository)
                .start(step)
                .listener(jobFailureNotificationListener)
                .build();
    }

    // ===== 2) Step (MSA) =====
    @Bean(name = "birthdayCouponStepMSA")
    public Step birthdayCouponStepMSA(
            @Qualifier("birthdayUserReaderMSA") ItemReader<BirthdayUserDto> reader,
            @Qualifier("birthdayUserWriterMSA") ItemWriter<BirthdayUserDto> writer
    ) {
        return new StepBuilder("birthdayCouponStepMSA", jobRepository)
                .<BirthdayUserDto, BirthdayUserDto>chunk(1000, transactionManager)
                .reader(reader)
                .writer(writer)
                .build();
    }

    // ===== 3) Reader (MSA): Feign 페이징 조회 =====
    @Bean(name = "birthdayUserReaderMSA")
    @StepScope
    public ItemReader<BirthdayUserDto> birthdayUserReaderMSA() {
        return new ItemReader<>() {
            private List<BirthdayUserDto> users = null;
            private int index = 0;

            private long lastSeenId = 0L; // 커서
            private final int size = 1000;
            private boolean finished = false;

            private final int month = LocalDate.now().getMonthValue();

            @Override
            public BirthdayUserDto read() {
                if (finished) return null; // 이미 끝났으면 null

                if (users == null || index >= users.size()) {
                    log.info("User 서버에서 {}월 생일자 조회(커서) - lastSeenId={}, size={}",
                            month, lastSeenId, size);
                    users = fetchWithRetry(month, lastSeenId, size);
                    index = 0;
                    // 더 이상 없으면 종료
                    if (users == null || users.isEmpty()) {
                        log.info("{}월 생일자 더 없음(빈 결과) - 종료 (lastSeenId={})", month, lastSeenId);
                        finished = true;
                        return null;
                    }

                    // 다음 요청을 위한 커서 갱신
                    long prevCursor = lastSeenId;
                    lastSeenId = users.get(users.size() - 1).getUserCreatedId();
                    log.info("{}월 생일자 조회 {}명 (cursor {} -> {})", month, users.size(), prevCursor, lastSeenId);
                }

                return users.get(index++);
            }

            private List<BirthdayUserDto> fetchWithRetry(int month, long lastSeenId, int size) {
                for (int attempt = 1; attempt <= 3; attempt++) {
                    try {
                        return userServiceClient.getBirthdayUsers(month, lastSeenId, size);
                    } catch (RetryableException e) {
                        if (attempt == 3){
                            throw new UserServicePagingFailedException(
                                    "User 서버 생일자 커서 조회 실패 (Retryable) " +
                                            "(month=" + month + ", lastSeenId=" + lastSeenId + ", size=" + size + ")", e);
                        }
                        log.warn("User 서버 커서 조회 재시도(Retryable) - attempt={}/3, month={}, lastSeenId={}",
                                attempt, month, lastSeenId);
                        sleep();
                    } catch (FeignException e) {
                        if (e.status() >= 400 && e.status() < 500) {
                            throw new UserServicePagingFailedException(
                                    "User 서버 생일자 커서 조회 실패 (4xx) (month=" + month +
                                            ", lastSeenId=" + lastSeenId + ", status=" + e.status() + ")", e);
                        }
                        if (attempt == 3) {
                            throw new UserServicePagingFailedException(
                                    "User 서버 생일자 커서 조회 실패 (5xx) (month=" + month +
                                            ", lastSeenId=" + lastSeenId + ", status=" + e.status() + ")", e);
                        }
                        sleep();
                    }
                }
                throw new IllegalStateException("User 서버 조회 실패");
            }

            private void sleep() {
                try {
                    Thread.sleep(300); } catch (InterruptedException ignored) {}
            }
        };
    }

    // ===== 4) Writer (MSA): RabbitMQ publish =====
    @Bean(name = "birthdayUserWriterMSA")
    public ItemWriter<BirthdayUserDto> birthdayUserWriterMSA() {
        return chunk -> {
            List<Long> userIds = chunk.getItems().stream()
                    .map(BirthdayUserDto::getUserCreatedId)
                    .toList();

            String batchKey = "birthday-" + LocalDate.now();

            BirthdayCouponBulkEvent event =
                    new BirthdayCouponBulkEvent(userIds, batchKey);

            CorrelationData cd = new CorrelationData(event.batchId());

            rabbitTemplate.convertAndSend(birthdayExchange, birthdayRoutingKey, event, message -> {
                message.getMessageProperties().setCorrelationId(event.batchId());
                return message;
            }, cd);

            CorrelationData.Confirm confirm = cd.getFuture().get(5, java.util.concurrent.TimeUnit.SECONDS);
            if (!confirm.isAck()) {
                throw new RabbitPublishFailedException(
                        "Rabbit publish NACK.(메시지 큐 접근 실패) batchId=" + event.batchId() + ", cause=" + confirm.getReason()
                );
            }
        };
    }

}
