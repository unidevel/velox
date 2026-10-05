/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/experimental/cudf/CudfConfig.h"
#include "velox/experimental/cudf/tests/utils/CudfHiveConnectorTestBase.h"

#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::cudf_velox::exec::test {
namespace {
using facebook::velox::exec::test::AssertQueryBuilder;
using facebook::velox::exec::test::PlanBuilder;

class TimestampWithTimeZoneTest : public CudfHiveConnectorTestBase {
 protected:
  // Checking the operator type is essential: fallback-disabled mode alone
  // permits retained CPU TableScan operators and cannot prove GPU execution.
  void assertGpuOperator(
      const std::shared_ptr<velox::exec::Task>& task,
      const core::PlanNodeId& id,
      const std::string& op) {
    auto stats = velox::exec::toPlanStats(task->taskStats());
    ASSERT_NE(stats.find(id), stats.end());
    SCOPED_TRACE(op);
    ASSERT_NE(
        stats.at(id).operatorStats.find(op), stats.at(id).operatorStats.end());
    EXPECT_GT(stats.at(id).operatorStats.at(op)->inputRows, 0);
  }
  void assertZonedRepresentatives(const RowVectorPtr& result) {
    auto values = result->childAt(0)->as<SimpleVector<int64_t>>();
    for (vector_size_t i = 0; i < result->size(); ++i) {
      if (values->isNullAt(i)) {
        continue;
      }
      auto value = values->valueAt(i);
      auto millis = unpackMillisUtc(value);
      if (millis == -1) {
        EXPECT_EQ(value, pack(-1, tz::getTimeZoneID("America/Los_Angeles")));
      } else if (millis == 1) {
        EXPECT_EQ(value, pack(1, tz::getTimeZoneID("Europe/Paris")));
      } else {
        EXPECT_EQ(millis, 0);
        EXPECT_TRUE(
            value == pack(0, tz::getTimeZoneID("UTC")) ||
            value == pack(0, tz::getTimeZoneID("America/Los_Angeles")) ||
            value == pack(0, tz::getTimeZoneID("Europe/Paris")) ||
            value == pack(0, tz::getTimeZoneID("Asia/Tokyo")));
      }
    }
  }
  RowVectorPtr zonedValues() {
    auto utc = tz::getTimeZoneID("UTC");
    auto la = tz::getTimeZoneID("America/Los_Angeles");
    auto paris = tz::getTimeZoneID("Europe/Paris");
    return makeRowVector(
        {"ts", "id"},
        {makeNullableFlatVector<int64_t>(
             {pack(0, la),
              pack(0, utc),
              pack(1, paris),
              pack(-1, la),
              std::nullopt},
             TIMESTAMP_WITH_TIME_ZONE()),
         makeFlatVector<int64_t>({1, 2, 3, 4, 5})});
  }
};

// TODO(delta): Compare UTC instants for zoned operands and constants, including
// AST/JIT parents.
TEST_F(TimestampWithTimeZoneTest, DISABLED_comparisonsAcrossTimezones) {
  auto& config = CudfConfig::getInstance();
  const auto oldAst = config.astExpressionEnabled;
  const auto oldJit = config.jitExpressionEnabled;
  SCOPE_EXIT {
    config.astExpressionEnabled = oldAst;
    config.jitExpressionEnabled = oldJit;
  };
  auto input = makeRowVector(
      {"a", "b", "raw_a", "raw_b"},
      {makeNullableFlatVector<int64_t>(
           {pack(0, 1), pack(1, 1), std::nullopt}, TIMESTAMP_WITH_TIME_ZONE()),
       makeFlatVector<int64_t>(
           {pack(0, 0), pack(1, 0), pack(0, 0)}, TIMESTAMP_WITH_TIME_ZONE()),
       makeNullableFlatVector<int64_t>({pack(0, 1), pack(1, 1), std::nullopt}),
       makeFlatVector<int64_t>({pack(0, 0), pack(1, 0), pack(0, 0)})});
  for (int mode = 0; mode < 3; ++mode) {
    SCOPED_TRACE(mode);
    config.astExpressionEnabled = mode == 1;
    config.jitExpressionEnabled = mode == 2;
    auto field = std::make_shared<core::FieldAccessTypedExpr>(
        TIMESTAMP_WITH_TIME_ZONE(), "a");
    auto literal = std::make_shared<core::ConstantTypedExpr>(
        TIMESTAMP_WITH_TIME_ZONE(),
        variant(pack(0, tz::getTimeZoneID("Europe/Paris"))));
    auto plan =
        PlanBuilder()
            .values({input})
            .project({"a", "(a = b) AND (raw_a <> raw_b)", "raw_a = raw_b"})
            .projectExpressions(
                {std::make_shared<core::FieldAccessTypedExpr>(BOOLEAN(), "p1"),
                 std::make_shared<core::FieldAccessTypedExpr>(BOOLEAN(), "p2"),
                 std::make_shared<core::CallTypedExpr>(
                     BOOLEAN(), "eq", field, literal),
                 std::make_shared<core::CallTypedExpr>(
                     BOOLEAN(), "lt", literal, field)})
            .planNode();
    auto task = AssertQueryBuilder(plan).assertResults(makeRowVector(
        {"p0", "p1", "p2", "p3"},
        {makeNullableFlatVector<bool>({true, true, std::nullopt}),
         makeNullableFlatVector<bool>({false, false, std::nullopt}),
         makeNullableFlatVector<bool>({true, false, std::nullopt}),
         makeNullableFlatVector<bool>({false, true, std::nullopt})}));
    assertGpuOperator(task, plan->id(), "CudfFilterProject");
  }
}

// TODO(delta): Group and partition equal UTC instants together while preserving
// an input zone.
TEST_F(TimestampWithTimeZoneTest, DISABLED_partitionedGroupingAcrossTimezones) {
  auto& config = CudfConfig::getInstance();
  const auto oldStreaming = config.streamingGroupbyEnabled;
  SCOPE_EXIT {
    config.streamingGroupbyEnabled = oldStreaming;
  };
  for (bool streaming : {false, true}) {
    SCOPED_TRACE(streaming);
    config.streamingGroupbyEnabled = streaming;
    auto ids = std::make_shared<core::PlanNodeIdGenerator>();
    std::vector<core::PlanNodePtr> sources;
    // Partial groups choose different zone representatives. Hash the instant
    // at the exchange so they reach the same final group, including nulls.
    for (auto zone : {"America/Los_Angeles", "Europe/Paris", "Asia/Tokyo"}) {
      auto input = makeRowVector(
          {"ts"},
          {makeNullableFlatVector<int64_t>(
              {pack(-1, tz::getTimeZoneID("America/Los_Angeles")),
               pack(0, tz::getTimeZoneID(zone)),
               pack(1, tz::getTimeZoneID("Europe/Paris")),
               std::nullopt},
              TIMESTAMP_WITH_TIME_ZONE())});
      sources.push_back(PlanBuilder(ids)
                            .values({input, input})
                            .partialAggregation({"ts"}, {"count(1)"})
                            .planNode());
    }
    core::PlanNodeId partitionId;
    auto plan = PlanBuilder(ids)
                    .localPartition({"ts"}, sources)
                    .capturePlanNodeId(partitionId)
                    .finalAggregation()
                    .planNode();
    auto expected = makeRowVector(
        {"ts", "a0"},
        {makeNullableFlatVector<int64_t>(
             {pack(-1, 0), pack(0, 0), pack(1, 0), std::nullopt},
             TIMESTAMP_WITH_TIME_ZONE()),
         makeFlatVector<int64_t>({6, 6, 6, 6})});
    std::shared_ptr<velox::exec::Task> task;
    auto result =
        AssertQueryBuilder(plan)
            .maxDrivers(3)
            .config(core::QueryConfig::kMaxLocalExchangePartitionCount, "3")
            .copyResults(pool_.get(), task);
    ASSERT_TRUE(
        facebook::velox::exec::test::assertEqualResults({expected}, {result}));
    assertZonedRepresentatives(result);
    assertGpuOperator(task, partitionId, "CudfLocalPartition");
    assertGpuOperator(
        task,
        plan->id(),
        std::string("CudfGroupby") +
            std::string(
                core::AggregationNode::toName(
                    core::AggregationNode::Step::kFinal)));
    if (streaming) {
      auto stats = velox::exec::toPlanStats(task->taskStats());
      EXPECT_GT(
          stats.at(plan->id()).customStats.at("streamingGroupbyUsed").sum, 0);
    }
  }
}

// TODO(delta): Deduplicate by UTC instant rather than the packed timezone key.
TEST_F(TimestampWithTimeZoneTest, DISABLED_distinctAcrossTimezones) {
  auto plan = PlanBuilder()
                  .values({zonedValues()})
                  .singleAggregation({"ts"}, {})
                  .planNode();
  auto expected = makeRowVector(
      {"ts"},
      {makeNullableFlatVector<int64_t>(
          {pack(-1, 0), pack(0, 0), pack(1, 0), std::nullopt},
          TIMESTAMP_WITH_TIME_ZONE())});
  std::shared_ptr<velox::exec::Task> task;
  auto result = AssertQueryBuilder(plan).copyResults(pool_.get(), task);
  ASSERT_TRUE(
      facebook::velox::exec::test::assertEqualResults({expected}, {result}));
  assertZonedRepresentatives(result);
  assertGpuOperator(
      task,
      plan->id(),
      std::string("CudfDistinct") +
          std::string(
              core::AggregationNode::toName(
                  core::AggregationNode::Step::kSingle)));
}

// TODO(delta): Remember UTC-instant keys across batches instead of packed zoned
// values.
TEST_F(TimestampWithTimeZoneTest, DISABLED_markDistinctAcrossBatches) {
  auto type = TIMESTAMP_WITH_TIME_ZONE();
  auto a = makeRowVector(
      {"ts"}, {makeFlatVector<int64_t>({pack(0, 0), pack(1, 0)}, type)});
  auto b = makeRowVector(
      {"ts"}, {makeFlatVector<int64_t>({pack(0, 1), pack(1, 1)}, type)});
  auto plan =
      PlanBuilder().values({a, b}).markDistinct("distinct", {"ts"}).planNode();
  auto task = AssertQueryBuilder(plan).assertResults(makeRowVector(
      {"ts", "distinct"},
      {makeFlatVector<int64_t>(
           {pack(0, 0), pack(1, 0), pack(0, 1), pack(1, 1)}, type),
       makeFlatVector<bool>({true, true, false, false})}));
  assertGpuOperator(task, plan->id(), "CudfMarkDistinct");
}

// TODO(delta): Join on UTC instants and preserve the original packed values in
// output.
TEST_F(TimestampWithTimeZoneTest, DISABLED_hashJoinAcrossTimezones) {
  auto type = TIMESTAMP_WITH_TIME_ZONE();
  auto left = makeRowVector(
      {"a", "l"},
      {makeNullableFlatVector<int64_t>(
           {pack(0, tz::getTimeZoneID("America/Los_Angeles")),
            pack(1, 0),
            std::nullopt},
           type),
       makeFlatVector<int64_t>({1, 2, 3})});
  auto right = makeRowVector(
      {"b", "r"},
      {makeNullableFlatVector<int64_t>(
           {pack(0, tz::getTimeZoneID("Europe/Paris")),
            pack(1, 0),
            std::nullopt},
           type),
       makeFlatVector<int64_t>({4, 5, 6})});
  for (const auto& filter : {"", "(a = b) AND (l < r)"}) {
    auto ids = std::make_shared<core::PlanNodeIdGenerator>();
    auto build = PlanBuilder(ids).values({right}).planNode();
    auto plan = PlanBuilder(ids)
                    .values({left})
                    .hashJoin({"a"}, {"b"}, build, filter, {"a", "b", "l", "r"})
                    .planNode();
    std::shared_ptr<velox::exec::Task> task;
    auto result = AssertQueryBuilder(plan).copyResults(pool_.get(), task);
    ASSERT_EQ(result->size(), 2);
    auto a = result->childAt(0)->as<SimpleVector<int64_t>>();
    auto b = result->childAt(1)->as<SimpleVector<int64_t>>();
    for (vector_size_t i = 0; i < result->size(); ++i) {
      EXPECT_EQ(type->compare(a->valueAt(i), b->valueAt(i)), 0);
      if (unpackMillisUtc(a->valueAt(i)) == 0) {
        EXPECT_EQ(
            unpackZoneKeyId(a->valueAt(i)),
            tz::getTimeZoneID("America/Los_Angeles"));
        EXPECT_EQ(
            unpackZoneKeyId(b->valueAt(i)), tz::getTimeZoneID("Europe/Paris"));
      }
    }
    assertGpuOperator(task, plan->id(), "CudfHashJoinProbe");
  }
}

// TODO(delta): Equal instants must tie on timestamp so secondary sort keys
// decide their order.
TEST_F(
    TimestampWithTimeZoneTest,
    DISABLED_sortingPreservesZonesAndUsesSecondaryKeys) {
  auto input = zonedValues();
  for (bool topN : {false, true}) {
    auto builder = PlanBuilder().values({input});
    if (topN) {
      builder.topN({"ts ASC NULLS LAST", "id ASC"}, 5, false);
    } else {
      builder.orderBy({"ts ASC NULLS LAST", "id ASC"}, false);
    }
    auto plan = builder.planNode();
    std::shared_ptr<velox::exec::Task> task;
    auto result = AssertQueryBuilder(plan).copyResults(pool_.get(), task);
    std::vector<int64_t> ids{4, 1, 2, 3, 5};
    auto actual = result->childAt(1)->as<SimpleVector<int64_t>>();
    ASSERT_EQ(result->size(), ids.size());
    for (size_t i = 0; i < ids.size(); ++i) {
      EXPECT_EQ(actual->valueAt(i), ids[i]);
    }
    auto keys = result->childAt(0)->as<SimpleVector<int64_t>>();
    EXPECT_EQ(
        unpackZoneKeyId(keys->valueAt(1)),
        tz::getTimeZoneID("America/Los_Angeles"));
    assertGpuOperator(task, plan->id(), topN ? "CudfTopN" : "CudfOrderBy");
  }
}
} // namespace
} // namespace facebook::velox::cudf_velox::exec::test
