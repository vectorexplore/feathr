import React, { forwardRef } from 'react'

import { DeleteOutlined } from '@ant-design/icons'
import { Button, Space, notification, Popconfirm, message } from 'antd'
import { useQuery } from 'react-query'
import { useNavigate } from 'react-router-dom'

import { fetchProjects, deleteEntity } from '@/api'
import ResizeTable, { ResizeColumnType } from '@/components/ResizeTable'
import { useStore } from '@/hooks'
import { Project } from '@/models/model'

export interface ProjectTableProps {
  project?: string
}
const ProjectTable = (props: ProjectTableProps, ref: any) => {
  const navigate = useNavigate()
  const { globalStore } = useStore()
  const { setProjectList } = globalStore
  const { project } = props

  const columns: ResizeColumnType<Project>[] = [
    {
      key: 'name',
      title: '项目名',
      dataIndex: 'name',
      resize: false
    },
    {
      key: 'action',
      title: '操作',
      width: 240,
      resize: false,
      render: (record: Project) => {
        const { name } = record
        return (
          <Space size="middle">
            <Button
              ghost
              type="primary"
              onClick={() => {
                navigate(`/${name}/features`)
              }}
            >
              查看特征
            </Button>
            <Button
              ghost
              type="primary"
              onClick={() => {
                navigate(`/${name}/lineage`)
              }}
            >
              查看血缘关系
            </Button>
            <Popconfirm
              title={`您确认要删除项目${name}吗？`}
              placement="topRight"
              onConfirm={() => {
                return new Promise((resolve) => {
                  onDelete(name, resolve)
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
  } = useQuery<Project[]>(
    ['Projects', project],
    async () => {
      const reuslt = await fetchProjects()
      setProjectList(reuslt)
      return reuslt.reduce((list, item: string) => {
        const text = project?.trim().toLocaleLowerCase()
        if (!text || item.includes(text)) {
          list.push({ name: item })
        }
        return list
      }, [] as Project[])
    },
    {
      retry: false,
      refetchOnWindowFocus: false
    }
  )

  const onDelete = async (entity: string, resolve: (value?: unknown) => void) => {
    try {
      await deleteEntity(entity)
      message.success('项目' + entity + '已经成功删除。')

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
      rowKey="name"
      loading={isLoading}
      columns={columns}
      dataSource={tableData}
      scroll={{ x: '100%' }}
    />
  )
}

const ProjectTableComponent = forwardRef<unknown, ProjectTableProps>(ProjectTable)

ProjectTableComponent.displayName = 'ProjectTableComponent'

export default ProjectTableComponent
