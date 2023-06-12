import React, { forwardRef } from 'react'

import { DeleteOutlined } from '@ant-design/icons'
import { Button, notification, message, Popconfirm, Space } from 'antd'
import { useQuery } from 'react-query'
import { useNavigate } from 'react-router-dom'

import { fetchFeatures, deleteEntity } from '@/api'
import ResizeTable, { ResizeColumnType } from '@/components/ResizeTable'
import { Feature } from '@/models/model'

export interface FeatureTableProps {
  project: string
  keyword?: string
}
const FeatureTable = (props: FeatureTableProps, ref: any) => {
  const navigate = useNavigate()

  const { project, keyword } = props

  const getDetialUrl = (guid: string) => {
    return `/${project}/features/${guid}`
  }

  const columns: ResizeColumnType<Feature>[] = [
    {
      key: 'name',
      title: '特征名称',
      ellipsis: true,
      width: 200,
      render: (record: Feature) => {
        return (
          <Button
            type="link"
            onClick={() => {
              navigate(getDetialUrl(record.guid))
            }}
          >
            {record.displayText}
          </Button>
        )
      }
    },
    {
      key: 'type',
      title: '类型',
      ellipsis: true,
      width: 120,
      render: (record: Feature) => {
        return record.typeName.replace(/feathr_|_v1/gi, '')
      }
    },
    {
      key: 'transformation',
      title: '特征变换',
      width: 220,
      render: (record: Feature) => {
        const { transformExpr, defExpr } = record.attributes.transformation
        return transformExpr || defExpr
      }
    },
    {
      key: 'entitykey',
      title: '主键',
      ellipsis: true,
      width: 120,
      render: (record: Feature) => {
        const key = record.attributes.key && record.attributes.key[0]
        if ('NOT_NEEDED' !== key.keyColumn) {
          return `${key.keyColumn} (${key.keyColumnType})`
        } else {
          return 'N/A'
        }
      }
    },
    {
      key: 'aggregation',
      title: '聚合方式',
      ellipsis: true,
      width: 150,
      render: (record: Feature) => {
        const { transformation } = record.attributes
        return (
          <>
            {transformation.aggFunc && `Type: ${transformation.aggFunc}`}
            <br />
            {transformation.aggFunc && `Window: ${transformation.window}`}
          </>
        )
      }
    },
    {
      title: '操作',
      fixed: 'right',
      width: 240,
      resize: false,
      render: (record: Feature) => {
        const { guid } = record
        return (
          <Space size="middle">
            <Button
              ghost
              type="primary"
              onClick={() => {
                navigate(getDetialUrl(guid))
              }}
            >
              查看详情
            </Button>
            <Popconfirm
              title="您确认要删除该特征吗？"
              placement="topRight"
              onConfirm={() => {
                return new Promise((resolve) => {
                  onDelete(guid, resolve)
                })
              }}
            >
              <Button danger ghost type="primary" icon={<DeleteOutlined />}>
                删除
              </Button>
            </Popconfirm>
          </Space>
        )
      }
    }
  ]

  const {
    isLoading,
    data: tableData,
    refetch
  } = useQuery<Feature[]>(
    ['dataSources', project, keyword],
    async () => {
      if (project) {
        return await fetchFeatures(project, 1, 10, keyword || '')
      } else {
        return []
      }
    },
    {
      retry: false,
      refetchOnWindowFocus: false
    }
  )

  const onDelete = async (entity: string, resolve: (value?: unknown) => void) => {
    try {
      await deleteEntity(entity)
      message.success('特征' + entity + '已经成功删除。')
      refetch()
    } catch (e: any) {
      notification.error({
        message: '',
        description: e.detail,
        placement: 'top'
      })
    } finally {
      resolve()
    }
  }

  return (
    <ResizeTable
      rowKey="guid"
      loading={isLoading}
      columns={columns}
      dataSource={tableData}
      scroll={{ x: '100%' }}
    />
  )
}

const FeatureTableComponent = forwardRef<unknown, FeatureTableProps>(FeatureTable)

FeatureTableComponent.displayName = 'FeatureTableComponent'

export default FeatureTableComponent
