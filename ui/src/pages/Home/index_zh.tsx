import React from 'react'

import { CopyOutlined, DatabaseOutlined, EyeOutlined, ProjectOutlined } from '@ant-design/icons'
import { Card, Col, Row, Typography } from 'antd'
import cs from 'classnames'
import { useNavigate } from 'react-router-dom'

import { observer, useStore } from '@/hooks'

import styles from './index.module.less'

const { Title, Link } = Typography
const { Meta } = Card

const features = [
  {
    icon: <ProjectOutlined style={{ color: '#177ddc' }} />,
    title: '项目',
    link: 'projects',
    linkText: '查看'
  },
  {
    icon: <DatabaseOutlined style={{ color: '#219ebc' }} />,
    title: '数据源',
    link: '/datasources',
    linkText: '查看'
  },
  {
    icon: <CopyOutlined style={{ color: '#ffb703' }} />,
    title: '特征',
    link: '/features',
    linkText: '查看'
  }
]

const Home = () => {
  const navigate = useNavigate()

  const { globalStore } = useStore()

  const { project, setSwitchProjecModalOpen } = globalStore

  const onSeeAll = (item: any) => {
    if (item.title === 'Projects') {
      navigate('/projects')
    } else if (project) {
      navigate(`/${project}${item.link}`)
    } else {
      setSwitchProjecModalOpen(true, item.title.toLocaleLowerCase())
    }
  }

  return (
    <div className={cs('page', styles.home)}>
      <Card>
        <Title level={2}>欢迎使用特征平台</Title>
        <span>
          您可以使用特征平台UI浏览和搜索特征、查找数据源、跟踪特征的血缘关系，以及管理权限。&nbsp;
          <a
            target="_blank"
            href="https://feathr-ai.github.io/feathr/concepts/feature-registry.html#accessing-feathr-ui"
            rel="noreferrer"
          >
            了解更多
          </a>
        </span>
      </Card>
      <Row gutter={16} style={{ marginTop: 16 }}>
        {features.map((item) => {
          return (
            <Col key={item.title} xl={6} lg={12} sm={24} xs={24} style={{ marginBottom: 16 }}>
              <Card>
                <Meta
                  title={
                    <Title ellipsis level={2}>
                      {item.title}
                    </Title>
                  }
                  description={
                    <Link
                      onClick={() => {
                        onSeeAll(item)
                      }}
                    >
                      {item.linkText}
                    </Link>
                  }
                  className={styles.cardMeta}
                  avatar={item.icon}
                />
              </Card>
            </Col>
          )
        })}
      </Row>
    </div>
  )
}

export default observer(Home)
