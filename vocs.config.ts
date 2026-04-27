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
    pattern: 'https://github.com/zannis/shove/edit/main/docs/pages/:path',
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
