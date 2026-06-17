import { typescript, javascript, github } from 'projen';
const project = new typescript.TypeScriptProject({
  defaultReleaseBranch: 'main',
  name: 'athena-query-result-collector',
  projenrcTs: true,
  authorName: 'yicr',
  authorEmail: 'yicr@users.noreply.github.com',
  typescriptVersion: '5.9.x',
  packageManager: javascript.NodePackageManager.YARN_CLASSIC,
  repository: 'https://github.com/gammarers-aws-sdk-extensions/athena-query-result-collector.git',
  description: 'A TypeScript library for collecting AWS Athena query results via pagination. It supports full collection, streaming, and page-based batch processing, and it uses athena-query-result-pager internally.',
  keywords: ['aws', 'athena', 'query', 'result', 'collector', 'pagination', 'retry', 'abort', 'signal'],
  releaseToNpm: true,
  npmTrustedPublishing: true,
  npmAccess: javascript.NpmAccess.PUBLIC,
  minNodeVersion: '20.0.0',
  workflowNodeVersion: '24.x',
  deps: [
    '@aws-sdk/client-athena@^3.983.0',
    'athena-query-result-pager@^0.4.0',
  ],
  depsUpgradeOptions: {
    workflowOptions: {
      labels: ['auto-approve', 'auto-merge'],
      schedule: javascript.UpgradeDependenciesSchedule.WEEKLY,
    },
  },
  githubOptions: {
    projenCredentials: github.GithubCredentials.fromApp({
      permissions: {
        pullRequests: github.workflows.AppPermission.WRITE,
        contents: github.workflows.AppPermission.WRITE,
        workflows: github.workflows.AppPermission.WRITE,
      },
    }),
  },
  autoApproveOptions: {
    allowedUsernames: [
      'gammarers-projen-upgrade-bot[bot]',
      'yicr',
    ],
  },
});
// package ignore .devcontainer directory
project.addPackageIgnore('/.devcontainer');

project.synth();