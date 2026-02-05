import { Card, Skeleton, Row, Col, Table } from 'antd';
import React from 'react';

interface PageSkeletonProps {
  rowCount?: number;
}

export function PageSkeleton({ rowCount = 3 }: PageSkeletonProps) {
  return (
    <div style={{ padding: 24 }}>
      <Skeleton.Input active style={{ width: 200, marginBottom: 24 }} />
      <Row gutter={[16, 16]}>
        {Array.from({ length: rowCount }).map((_, index) => (
          <Col key={index} span={24}>
            <Card>
              <Skeleton.Input active style={{ marginBottom: 12 }} />
              <Skeleton.Input active style={{ width: '60%', marginBottom: 12 }} />
              <Skeleton paragraph active rows={3} />
            </Card>
          </Col>
        ))}
      </Row>
    </div>
  );
}

interface TableSkeletonProps {
  rowCount?: number;
  columnCount?: number;
}

export function TableSkeleton({ rowCount = 10, columnCount = 6 }: TableSkeletonProps) {
  return (
    <div style={{ padding: 24 }}>
      <Skeleton.Input active style={{ width: 200, marginBottom: 16 }} />
      <Skeleton.Input active style={{ width: 400, marginBottom: 16 }} />
      <Table
        dataSource={Array.from({ length: rowCount })}
        pagination={false}
        locale={{ emptyText: ' ' }}
        columns={Array.from({ length: columnCount }).map((_, index) => ({
          title: <Skeleton.Input active />,
          dataIndex: 'key',
          key: index,
          render: () => <Skeleton.Input active size="small" />,
        }))}
      />
    </div>
  );
}

interface CardSkeletonProps {
  rows?: number;
}

export function CardSkeleton({ rows = 3 }: CardSkeletonProps) {
  return (
    <Card>
      <Skeleton.Input active style={{ width: '40%', marginBottom: 16 }} />
      {Array.from({ length: rows }).map((_, index) => (
        <Skeleton key={index} paragraph={{ rows: 1 }} style={{ marginBottom: 8 }} />
      ))}
    </Card>
  );
}

export default PageSkeleton;
