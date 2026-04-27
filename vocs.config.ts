import { defineConfig } from 'vocs'

export default defineConfig({
  title: 'shove',
  titleTemplate: '%s · shove',
  description:
    'Type-safe async pub/sub for Rust on RabbitMQ, AWS SNS+SQS, NATS JetStream, Apache Kafka, or in-process.',
  iconUrl: { light: '/shove-icon.svg', dark: '/shove-icon-dark.svg' },
  logoUrl: { light: '/shove-lockup.svg', dark: '/shove-lockup-dark.svg' },
  ogImageUrl: 'https://shove.rs/og-image.png',
  rootDir: 'docs',
  theme: {
    accentColor: { light: '#CE422B', dark: '#F97316' },
  },
  topNav: [
    { text: 'Docs', link: '/getting-started' },
    { text: 'Reference', link: 'https://docs.rs/shove' },
    { text: 'crates.io', link: 'https://crates.io/crates/shove' },
    { text: 'GitHub', link: 'https://github.com/zannis/shove' },
  ],
  socials: [
    { icon: 'github', link: 'https://github.com/zannis/shove' },
  ],
  editLink: {
    pattern: (pageData) => {
      const fp = pageData.filePath ?? ''
      const exampleMap: Record<string, string> = {
        'backends/rabbitmq/examples/basic.mdx': 'examples/rabbitmq/basic_pubsub.rs',
        'backends/rabbitmq/examples/sequenced.mdx': 'examples/rabbitmq/sequenced_pubsub.rs',
        'backends/rabbitmq/examples/consumer-groups.mdx': 'examples/rabbitmq/consumer_groups.rs',
        'backends/rabbitmq/examples/audited.mdx': 'examples/rabbitmq/audited_consumer.rs',
        'backends/rabbitmq/examples/concurrent.mdx': 'examples/rabbitmq/concurrent_pubsub.rs',
        'backends/rabbitmq/examples/exactly-once.mdx': 'examples/rabbitmq/exactly_once.rs',
        'backends/rabbitmq/examples/stress.mdx': 'examples/rabbitmq/stress.rs',
        'backends/sqs/examples/basic.mdx': 'examples/sqs/basic_pubsub.rs',
        'backends/sqs/examples/sequenced.mdx': 'examples/sqs/sequenced_pubsub.rs',
        'backends/sqs/examples/concurrent.mdx': 'examples/sqs/concurrent_pubsub.rs',
        'backends/sqs/examples/consumer-groups.mdx': 'examples/sqs/consumer_groups.rs',
        'backends/sqs/examples/audited.mdx': 'examples/sqs/audited_consumer.rs',
        'backends/sqs/examples/autoscaler.mdx': 'examples/sqs/autoscaler.rs',
        'backends/sqs/examples/stress.mdx': 'examples/sqs/stress.rs',
        'backends/nats/examples/basic.mdx': 'examples/nats/basic.rs',
        'backends/nats/examples/sequenced.mdx': 'examples/nats/sequenced.rs',
        'backends/nats/examples/audited.mdx': 'examples/nats/audited_consumer.rs',
        'backends/nats/examples/stress.mdx': 'examples/nats/stress.rs',
        'backends/kafka/examples/basic.mdx': 'examples/kafka/basic.rs',
        'backends/kafka/examples/sequenced.mdx': 'examples/kafka/sequenced.rs',
        'backends/kafka/examples/audited.mdx': 'examples/kafka/audited_consumer.rs',
        'backends/kafka/examples/stress.mdx': 'examples/kafka/stress.rs',
        'backends/inmemory/examples/basic.mdx': 'examples/inmemory/basic.rs',
        'backends/inmemory/examples/sequenced.mdx': 'examples/inmemory/sequenced.rs',
        'backends/inmemory/examples/consumer-groups.mdx': 'examples/inmemory/consumer_groups.rs',
        'backends/inmemory/examples/audited.mdx': 'examples/inmemory/audited_consumer.rs',
        'backends/inmemory/examples/stress.mdx': 'examples/inmemory/stress.rs',
      }
      const target = exampleMap[fp] ?? `docs/pages/${fp}`
      return `https://github.com/zannis/shove/edit/main/${target}`
    },
    text: 'Edit on GitHub',
  },
  sidebar: [
    { text: 'Introduction', link: '/' },
    { text: 'Getting Started', link: '/getting-started' },
    {
      text: 'Core Concepts',
      collapsed: false,
      items: [
        { text: 'Topics & Topology', link: '/concepts/topics' },
        { text: 'Outcomes & Delivery', link: '/concepts/outcomes' },
        { text: 'Handlers & Context', link: '/concepts/handlers' },
        { text: 'The Broker<B> Pattern', link: '/concepts/broker' },
      ],
    },
    {
      text: 'Backends',
      collapsed: false,
      items: [
        { text: 'Choosing a Backend', link: '/backends/choosing' },
        {
          text: 'RabbitMQ',
          collapsed: true,
          items: [
            { text: 'Overview', link: '/backends/rabbitmq' },
            { text: 'Basic Pubsub', link: '/backends/rabbitmq/examples/basic' },
            { text: 'Sequenced Pubsub', link: '/backends/rabbitmq/examples/sequenced' },
            { text: 'Consumer Groups', link: '/backends/rabbitmq/examples/consumer-groups' },
            { text: 'Audited Consumer', link: '/backends/rabbitmq/examples/audited' },
            { text: 'Concurrent Pubsub', link: '/backends/rabbitmq/examples/concurrent' },
            { text: 'Exactly-Once', link: '/backends/rabbitmq/examples/exactly-once' },
            { text: 'Stress', link: '/backends/rabbitmq/examples/stress' },
          ],
        },
        {
          text: 'AWS SNS + SQS',
          collapsed: true,
          items: [
            { text: 'Overview', link: '/backends/sqs' },
            { text: 'Basic Pubsub', link: '/backends/sqs/examples/basic' },
            { text: 'Sequenced Pubsub', link: '/backends/sqs/examples/sequenced' },
            { text: 'Concurrent Pubsub', link: '/backends/sqs/examples/concurrent' },
            { text: 'Consumer Groups', link: '/backends/sqs/examples/consumer-groups' },
            { text: 'Audited Consumer', link: '/backends/sqs/examples/audited' },
            { text: 'Autoscaler', link: '/backends/sqs/examples/autoscaler' },
            { text: 'Stress', link: '/backends/sqs/examples/stress' },
          ],
        },
        {
          text: 'NATS JetStream',
          collapsed: true,
          items: [
            { text: 'Overview', link: '/backends/nats' },
            { text: 'Basic', link: '/backends/nats/examples/basic' },
            { text: 'Sequenced', link: '/backends/nats/examples/sequenced' },
            { text: 'Audited Consumer', link: '/backends/nats/examples/audited' },
            { text: 'Stress', link: '/backends/nats/examples/stress' },
          ],
        },
        {
          text: 'Apache Kafka',
          collapsed: true,
          items: [
            { text: 'Overview', link: '/backends/kafka' },
            { text: 'Basic', link: '/backends/kafka/examples/basic' },
            { text: 'Sequenced', link: '/backends/kafka/examples/sequenced' },
            { text: 'Audited Consumer', link: '/backends/kafka/examples/audited' },
            { text: 'Stress', link: '/backends/kafka/examples/stress' },
          ],
        },
        {
          text: 'In-process',
          collapsed: true,
          items: [
            { text: 'Overview', link: '/backends/inmemory' },
            { text: 'Basic', link: '/backends/inmemory/examples/basic' },
            { text: 'Sequenced', link: '/backends/inmemory/examples/sequenced' },
            { text: 'Consumer Groups', link: '/backends/inmemory/examples/consumer-groups' },
            { text: 'Audited Consumer', link: '/backends/inmemory/examples/audited' },
            { text: 'Stress', link: '/backends/inmemory/examples/stress' },
          ],
        },
      ],
    },
    {
      text: 'Guides',
      collapsed: false,
      items: [
        { text: 'Sequenced Topics', link: '/guides/sequenced' },
        { text: 'Retries, Hold Queues & DLQs', link: '/guides/retries' },
        { text: 'Audit Logging', link: '/guides/audit' },
        { text: 'Consumer Groups & Autoscaling', link: '/guides/groups' },
        { text: 'Exactly-Once (RabbitMQ)', link: '/guides/exactly-once' },
        { text: 'Shutdown & Exit Codes', link: '/guides/shutdown' },
        { text: 'Observability', link: '/guides/observability' },
      ],
    },
    {
      text: 'Operations',
      collapsed: false,
      items: [
        { text: 'Performance Tuning', link: '/ops/performance' },
        { text: 'Backend Ops Notes', link: '/ops/backends' },
      ],
    },
    { text: 'Reference', link: '/reference' },
  ],
})
