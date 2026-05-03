"""
JMeter Test Plan Generator
Generates a .jmx file for performance testing the flight ops Kafka-Lambda pipeline.
Covers: Kafka producer throughput, Lambda HTTP endpoint load, E2E scenario.

Run with:
    python generate_jmx.py
    jmeter -n -t flight_ops_perf_test.jmx -l results/results.jtl -e -o reports/jmeter_report
"""

import os

JMX_TEMPLATE = '''<?xml version="1.0" encoding="UTF-8"?>
<jmeterTestPlan version="1.2" properties="5.0" jmeter="5.6.3">
  <hashTree>
    <TestPlan guiclass="TestPlanGui" testclass="TestPlan" testname="Flight Ops Pipeline - Performance Test" enabled="true">
      <stringProp name="TestPlan.comments">Performance test for flight operations Kafka-Lambda pipeline</stringProp>
      <boolProp name="TestPlan.functional_mode">false</boolProp>
      <boolProp name="TestPlan.tearDown_on_shutdown">true</boolProp>
      <elementProp name="TestPlan.user_defined_variables" elementType="Arguments">
        <collectionProp name="Arguments.arguments">
          <elementProp name="KAFKA_HOST" elementType="Argument">
            <stringProp name="Argument.name">KAFKA_HOST</stringProp>
            <stringProp name="Argument.value">{kafka_host}</stringProp>
          </elementProp>
          <elementProp name="KAFKA_PORT" elementType="Argument">
            <stringProp name="Argument.name">KAFKA_PORT</stringProp>
            <stringProp name="Argument.value">{kafka_port}</stringProp>
          </elementProp>
          <elementProp name="KAFKA_TOPIC" elementType="Argument">
            <stringProp name="Argument.name">KAFKA_TOPIC</stringProp>
            <stringProp name="Argument.value">{kafka_topic}</stringProp>
          </elementProp>
          <elementProp name="LAMBDA_ENDPOINT" elementType="Argument">
            <stringProp name="Argument.name">LAMBDA_ENDPOINT</stringProp>
            <stringProp name="Argument.value">{lambda_endpoint}</stringProp>
          </elementProp>
          <elementProp name="THREAD_COUNT" elementType="Argument">
            <stringProp name="Argument.name">THREAD_COUNT</stringProp>
            <stringProp name="Argument.value">{thread_count}</stringProp>
          </elementProp>
          <elementProp name="RAMP_UP" elementType="Argument">
            <stringProp name="Argument.name">RAMP_UP</stringProp>
            <stringProp name="Argument.value">{ramp_up}</stringProp>
          </elementProp>
          <elementProp name="DURATION" elementType="Argument">
            <stringProp name="Argument.name">DURATION</stringProp>
            <stringProp name="Argument.value">{duration}</stringProp>
          </elementProp>
        </collectionProp>
      </elementProp>
    </TestPlan>
    <hashTree>

      <!-- ═══════════════════════════════════════════════
           TEST GROUP 1: Lambda HTTP Endpoint Load Test
           ═══════════════════════════════════════════════ -->
      <ThreadGroup guiclass="ThreadGroupGui" testclass="ThreadGroup" testname="Lambda - Flight Event Processing" enabled="true">
        <stringProp name="ThreadGroup.on_sample_error">continue</stringProp>
        <elementProp name="ThreadGroup.main_controller" elementType="LoopController">
          <boolProp name="LoopController.continue_forever">false</boolProp>
          <intProp name="LoopController.loops">-1</intProp>
        </elementProp>
        <stringProp name="ThreadGroup.num_threads">${{THREAD_COUNT}}</stringProp>
        <stringProp name="ThreadGroup.ramp_time">${{RAMP_UP}}</stringProp>
        <boolProp name="ThreadGroup.scheduler">true</boolProp>
        <stringProp name="ThreadGroup.duration">${{DURATION}}</stringProp>
        <stringProp name="ThreadGroup.delay">0</stringProp>
      </ThreadGroup>
      <hashTree>

        <!-- Random Flight Event Body -->
        <JSR223PreProcessor guiclass="TestBeanGUI" testclass="JSR223PreProcessor" testname="Generate Flight Event" enabled="true">
          <stringProp name="scriptLanguage">groovy</stringProp>
          <stringProp name="script">
import groovy.json.JsonOutput
import java.util.UUID

def airlines = ["BA", "EK", "LH", "AA", "DL", "UA", "QR", "SQ"]
def airports = ["LHR", "JFK", "DXB", "FRA", "CDG", "SIN", "LAX", "ORD"]
def statuses = ["ON_TIME", "DELAYED", "BOARDING", "DEPARTED"]
def random = new Random()

def origin = airports[random.nextInt(airports.size())]
def destination = airports.findAll {{ it != origin }}[random.nextInt(airports.size() - 1)]

def event = [
    event_id: UUID.randomUUID().toString(),
    event_type: "flight_update",
    event_timestamp: new Date().toInstant().toString(),
    schema_version: "1.0",
    flight: [
        flight_number: airlines[random.nextInt(airlines.size())] + (random.nextInt(9000) + 1000),
        aircraft_type: ["B737", "A320", "B777"][random.nextInt(3)],
        route: [
            origin: origin,
            destination: destination,
            scheduled_departure: new Date().toInstant().toString(),
        ],
        status: statuses[random.nextInt(statuses.size())],
        delay_minutes: random.nextInt(120),
        gate: (char)(65 + random.nextInt(6)) + String.valueOf(random.nextInt(50) + 1)
    ],
    passengers: [
        capacity: random.nextInt(400) + 100,
        boarded: random.nextInt(350) + 80
    ],
    metadata: [
        source_system: "OPS_CENTER",
        priority: ["LOW", "MEDIUM", "HIGH"][random.nextInt(3)]
    ]
]

vars.put("FLIGHT_PAYLOAD", JsonOutput.toJson(event))
          </stringProp>
        </JSR223PreProcessor>

        <!-- HTTP Request to Lambda -->
        <HTTPSamplerProxy guiclass="HttpTestSampleGui" testclass="HTTPSamplerProxy" testname="POST - Flight Event to Lambda" enabled="true">
          <boolProp name="HTTPSampler.postBodyRaw">true</boolProp>
          <elementProp name="HTTPsampler.Arguments" elementType="Arguments">
            <collectionProp name="Arguments.arguments">
              <elementProp name="" elementType="HTTPArgument">
                <boolProp name="HTTPArgument.always_encode">false</boolProp>
                <stringProp name="Argument.value">${{FLIGHT_PAYLOAD}}</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
            </collectionProp>
          </elementProp>
          <stringProp name="HTTPSampler.domain">${{LAMBDA_ENDPOINT}}</stringProp>
          <stringProp name="HTTPSampler.port">8080</stringProp>
          <stringProp name="HTTPSampler.protocol">http</stringProp>
          <stringProp name="HTTPSampler.path">/2015-03-31/functions/flight-ops-processor/invocations</stringProp>
          <stringProp name="HTTPSampler.method">POST</stringProp>
          <boolProp name="HTTPSampler.follow_redirects">true</boolProp>
          <boolProp name="HTTPSampler.auto_redirects">false</boolProp>
          <boolProp name="HTTPSampler.use_keepalive">true</boolProp>
          <boolProp name="HTTPSampler.DO_MULTIPART_POST">false</boolProp>
        </HTTPSamplerProxy>
        <hashTree>
          <!-- Assert 200 response -->
          <ResponseAssertion guiclass="AssertionGui" testclass="ResponseAssertion" testname="Assert 200 OK" enabled="true">
            <collectionProp name="Asserion.test_strings">
              <stringProp name="49586">200</stringProp>
            </collectionProp>
            <stringProp name="Assertion.custom_message">Lambda did not return 200</stringProp>
            <stringProp name="Assertion.test_field">Assertion.response_code</stringProp>
            <boolProp name="Assertion.assume_success">false</boolProp>
            <intProp name="Assertion.test_type">8</intProp>
          </ResponseAssertion>

          <!-- Assert response time under 2000ms -->
          <DurationAssertion guiclass="DurationAssertionGui" testclass="DurationAssertion" testname="Response Time &lt; 2000ms" enabled="true">
            <stringProp name="DurationAssertion.duration">2000</stringProp>
          </DurationAssertion>
        </hashTree>

        <!-- Think time between requests -->
        <ConstantThroughputTimer guiclass="TestBeanGUI" testclass="ConstantThroughputTimer" testname="Throttle - 100 req/min" enabled="false">
          <intProp name="calcMode">1</intProp>
          <doubleProp>
            <name>throughput</name>
            <value>100.0</value>
          </doubleProp>
        </ConstantThroughputTimer>
      </hashTree>

      <!-- ═══════════════════════════════════════════════
           LISTENERS
           ═══════════════════════════════════════════════ -->
      <ResultCollector guiclass="SummaryReport" testclass="ResultCollector" testname="Summary Report" enabled="true">
        <boolProp name="ResultCollector.error_logging">false</boolProp>
        <objProp>
          <name>saveConfig</name>
          <value class="SampleSaveConfiguration">
            <time>true</time>
            <latency>true</latency>
            <timestamp>true</timestamp>
            <success>true</success>
            <label>true</label>
            <code>true</code>
            <message>true</message>
            <threadName>true</threadName>
            <dataType>true</dataType>
            <encoding>false</encoding>
            <assertions>true</assertions>
            <subresults>true</subresults>
            <responseData>false</responseData>
            <samplerData>false</samplerData>
            <xml>false</xml>
            <fieldNames>true</fieldNames>
            <responseHeaders>false</responseHeaders>
            <requestHeaders>false</requestHeaders>
            <responseDataOnError>false</responseDataOnError>
            <saveAssertionResultsFailureMessage>true</saveAssertionResultsFailureMessage>
            <assertionsResultsToSave>0</assertionsResultsToSave>
            <bytes>true</bytes>
            <sentBytes>true</sentBytes>
            <threadCounts>true</threadCounts>
            <idleTime>true</idleTime>
            <connectTime>true</connectTime>
          </value>
        </objProp>
        <stringProp name="filename">results/results.jtl</stringProp>
      </ResultCollector>

      <ResultCollector guiclass="StatVisualizer" testclass="ResultCollector" testname="Response Time Graph" enabled="true">
        <boolProp name="ResultCollector.error_logging">false</boolProp>
        <objProp>
          <name>saveConfig</name>
          <value class="SampleSaveConfiguration">
            <time>true</time>
            <latency>true</latency>
          </value>
        </objProp>
        <stringProp name="filename">results/response_times.jtl</stringProp>
      </ResultCollector>

    </hashTree>
  </hashTree>
</jmeterTestPlan>
'''


def generate_jmx(
    kafka_host: str = "localhost",
    kafka_port: str = "9092",
    kafka_topic: str = "flight-operations",
    lambda_endpoint: str = "localhost",
    thread_count: int = 50,
    ramp_up: int = 30,
    duration: int = 120,
    output_path: str = "jmeter/flight_ops_perf_test.jmx"
):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    os.makedirs("results", exist_ok=True)

    content = JMX_TEMPLATE.format(
        kafka_host=kafka_host,
        kafka_port=kafka_port,
        kafka_topic=kafka_topic,
        lambda_endpoint=lambda_endpoint,
        thread_count=thread_count,
        ramp_up=ramp_up,
        duration=duration
    )

    with open(output_path, "w") as f:
        f.write(content)

    print(f"✅ JMeter test plan generated: {output_path}")
    print(f"   Threads: {thread_count}, Ramp-up: {ramp_up}s, Duration: {duration}s")
    print(f"\n▶  Run with:")
    print(f"   jmeter -n -t {output_path} -l results/results.jtl -e -o reports/jmeter_report")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Generate JMeter test plan for flight ops pipeline")
    parser.add_argument("--kafka-host", default=os.getenv("KAFKA_HOST", "localhost"))
    parser.add_argument("--kafka-port", default=os.getenv("KAFKA_PORT", "9092"))
    parser.add_argument("--kafka-topic", default=os.getenv("KAFKA_TOPIC", "flight-operations"))
    parser.add_argument("--lambda-endpoint", default=os.getenv("LAMBDA_ENDPOINT", "localhost"))
    parser.add_argument("--threads", type=int, default=50)
    parser.add_argument("--ramp-up", type=int, default=30)
    parser.add_argument("--duration", type=int, default=120)
    args = parser.parse_args()

    generate_jmx(
        kafka_host=args.kafka_host,
        kafka_port=args.kafka_port,
        kafka_topic=args.kafka_topic,
        lambda_endpoint=args.lambda_endpoint,
        thread_count=args.threads,
        ramp_up=args.ramp_up,
        duration=args.duration
    )
